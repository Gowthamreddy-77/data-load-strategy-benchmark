import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
import duckdb
from src.schema import DDL_STATEMENTS
from src.validator import validate_and_quarantine

class IngestionEngine:
    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)
        for ddl in DDL_STATEMENTS.values():
            self.con.execute(ddl)

    def log_audit(self, run_id, strategy, batch_id, start_time, end_time, input_cnt, valid_cnt, rej_cnt, target_cnt, status, error_msg=""):
        dur = (end_time - start_time).total_seconds() * 1000.0
        self.con.execute(
            "INSERT INTO meta_audit_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [run_id, strategy, batch_id, start_time, end_time, dur, input_cnt, valid_cnt, rej_cnt, target_cnt, status, error_msg]
        )

    def save_quarantine(self, quarantined_records: List[Dict[str, Any]]):
        if quarantined_records:
            rows = [
                (q["run_id"], q["batch_id"], q["order_id"], q["raw_payload"], q["rejection_reason"], q["rejected_at"])
                for q in quarantined_records
            ]
            self.con.executemany("INSERT INTO raw_quarantine VALUES (?, ?, ?, ?, ?, ?)", rows)

    def _rows_to_params(self, records: List[Dict[str, Any]]):
        return [
            (
                r["order_id"],
                r["customer_id"],
                r["order_date"],
                r["partition_date"],
                float(r["amount"]),
                r["status"],
                r["updated_at"],
                r["batch_run_id"],
            )
            for r in records
        ]

    def load_full_refresh(self, all_raw: List[Dict[str, Any]], batch_id: str, run_id: str = None) -> str:
        run_id = run_id or str(uuid.uuid4())
        start = datetime.now(timezone.utc)
        valid, quarantined = validate_and_quarantine(all_raw, run_id, batch_id)
        self.save_quarantine(quarantined)
        try:
            self.con.begin()
            self.con.execute("DELETE FROM target_orders_full")
            if valid:
                rows = self._rows_to_params(valid)
                self.con.executemany(
                    """
                    INSERT INTO target_orders_full 
                    VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, CAST(? AS TIMESTAMP), ?)
                    """,
                    rows,
                )
            self.con.commit()
            cnt = self.con.execute("SELECT COUNT(*) FROM target_orders_full").fetchone()[0]
            self.log_audit(run_id, "FULL_REFRESH", batch_id, start, datetime.now(timezone.utc), len(all_raw), len(valid), len(quarantined), cnt, "SUCCESS")
            return run_id
        except Exception as e:
            self.con.rollback()
            self.log_audit(run_id, "FULL_REFRESH", batch_id, start, datetime.now(timezone.utc), len(all_raw), len(valid), len(quarantined), 0, "FAILED", str(e))
            raise e

    def load_incremental_merge(self, delta_raw: List[Dict[str, Any]], batch_id: str, run_id: str = None) -> str:
        run_id = run_id or str(uuid.uuid4())
        start = datetime.now(timezone.utc)
        valid, quarantined = validate_and_quarantine(delta_raw, run_id, batch_id)
        self.save_quarantine(quarantined)
        try:
            self.con.begin()
            if valid:
                self.con.execute("""
                    CREATE TEMP TABLE stage_merge (
                        order_id VARCHAR, customer_id VARCHAR, order_date DATE,
                        partition_date DATE, amount DOUBLE, status VARCHAR,
                        updated_at TIMESTAMP, batch_run_id VARCHAR
                    );
                """)
                rows = self._rows_to_params(valid)
                self.con.executemany(
                    "INSERT INTO stage_merge VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, CAST(? AS TIMESTAMP), ?)",
                    rows,
                )
                self.con.execute("DELETE FROM target_orders_merge WHERE order_id IN (SELECT order_id FROM stage_merge);")
                self.con.execute("INSERT INTO target_orders_merge SELECT * FROM stage_merge;")
                self.con.execute("DROP TABLE stage_merge;")
            self.con.commit()
            cnt = self.con.execute("SELECT COUNT(*) FROM target_orders_merge").fetchone()[0]
            self.log_audit(run_id, "INCREMENTAL_MERGE", batch_id, start, datetime.now(timezone.utc), len(delta_raw), len(valid), len(quarantined), cnt, "SUCCESS")
            return run_id
        except Exception as e:
            self.con.rollback()
            self.log_audit(run_id, "INCREMENTAL_MERGE", batch_id, start, datetime.now(timezone.utc), len(delta_raw), len(valid), len(quarantined), 0, "FAILED", str(e))
            raise e

    def load_partition_replacement(self, delta_raw: List[Dict[str, Any]], batch_id: str, run_id: str = None) -> str:
        run_id = run_id or str(uuid.uuid4())
        start = datetime.now(timezone.utc)
        valid, quarantined = validate_and_quarantine(delta_raw, run_id, batch_id)
        self.save_quarantine(quarantined)
        try:
            self.con.begin()
            if valid:
                self.con.execute("""
                    CREATE TEMP TABLE stage_part (
                        order_id VARCHAR, customer_id VARCHAR, order_date DATE,
                        partition_date DATE, amount DOUBLE, status VARCHAR,
                        updated_at TIMESTAMP, batch_run_id VARCHAR
                    );
                """)
                rows = self._rows_to_params(valid)
                self.con.executemany(
                    "INSERT INTO stage_part VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, CAST(? AS TIMESTAMP), ?)",
                    rows,
                )
                
                # 1. Purge previous occurrences of incoming order_ids across all historical partitions
                self.con.execute("""
                    DELETE FROM target_orders_partition 
                    WHERE order_id IN (SELECT order_id FROM stage_part);
                """)

                # 2. Combine with untouched records within the touched partition slices
                self.con.execute("""
                    CREATE TEMP TABLE stage_combined_partitions AS
                    SELECT * FROM stage_part
                    UNION ALL
                    SELECT t.* FROM target_orders_partition t
                    WHERE t.partition_date IN (SELECT DISTINCT partition_date FROM stage_part);
                """)

                # 3. Atomically overwrite affected partition dates
                self.con.execute("""
                    DELETE FROM target_orders_partition 
                    WHERE partition_date IN (SELECT DISTINCT partition_date FROM stage_part);
                """)
                self.con.execute("INSERT INTO target_orders_partition SELECT * FROM stage_combined_partitions;")
                self.con.execute("DROP TABLE stage_part;")
                self.con.execute("DROP TABLE stage_combined_partitions;")
            self.con.commit()
            cnt = self.con.execute("SELECT COUNT(*) FROM target_orders_partition").fetchone()[0]
            self.log_audit(run_id, "PARTITION_REPLACE", batch_id, start, datetime.now(timezone.utc), len(delta_raw), len(valid), len(quarantined), cnt, "SUCCESS")
            return run_id
        except Exception as e:
            self.con.rollback()
            self.log_audit(run_id, "PARTITION_REPLACE", batch_id, start, datetime.now(timezone.utc), len(delta_raw), len(valid), len(quarantined), 0, "FAILED", str(e))
            raise e
