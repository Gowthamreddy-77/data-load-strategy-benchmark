from tabulate import tabulate
import duckdb

def run_reconciliation(con: duckdb.DuckDBPyConnection):
    cnt_f = con.execute("SELECT COUNT(*) FROM target_orders_full").fetchone()[0]
    cnt_m = con.execute("SELECT COUNT(*) FROM target_orders_merge").fetchone()[0]
    cnt_p = con.execute("SELECT COUNT(*) FROM target_orders_partition").fetchone()[0]

    sum_f = con.execute("SELECT ROUND(SUM(amount), 2) FROM target_orders_full").fetchone()[0]
    sum_m = con.execute("SELECT ROUND(SUM(amount), 2) FROM target_orders_merge").fetchone()[0]
    sum_p = con.execute("SELECT ROUND(SUM(amount), 2) FROM target_orders_partition").fetchone()[0]

    diff_fm = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id, customer_id, order_date, amount, status FROM target_orders_full
            EXCEPT
            SELECT order_id, customer_id, order_date, amount, status FROM target_orders_merge
        )
    """).fetchone()[0]

    diff_fp = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id, customer_id, order_date, amount, status FROM target_orders_full
            EXCEPT
            SELECT order_id, customer_id, order_date, amount, status FROM target_orders_partition
        )
    """).fetchone()[0]

    return {
        "is_equivalent": (cnt_f == cnt_m == cnt_p) and (sum_f == sum_m == sum_p) and (diff_fm == 0) and (diff_fp == 0),
        "counts": {"full": cnt_f, "merge": cnt_m, "partition": cnt_p},
        "sum_amount": {"full": sum_f, "merge": sum_m, "partition": sum_p},
        "differences": diff_fm + diff_fp,
    }

def print_comparison_tables(con: duckdb.DuckDBPyConnection):
    audit_rows = con.execute("SELECT strategy, batch_id, duration_ms, input_count, valid_count, rejected_count, target_count_after, status FROM meta_audit_log ORDER BY start_time ASC").fetchall()
    headers = ["Strategy", "Batch", "Time (ms)", "Input Rows", "Valid", "Rejects", "Target Total", "Status"]
    print("\n================== OPERATIONAL EXECUTION METRICS ==================")
    print(tabulate(audit_rows, headers=headers, tablefmt="github"))

    recon = run_reconciliation(con)
    print("\n==================== DATA RECONCILIATION REPORT ====================")
    print(f"Data Parity: {'PASSED (EQUIVALENT)' if recon['is_equivalent'] else 'FAILED'}")
    print(f"Row Counts (Full / Merge / Partition): {recon['counts']['full']} / {recon['counts']['merge']} / {recon['counts']['partition']}")
    print(f"Amount Checksum: ${recon['sum_amount']['full']} / ${recon['sum_amount']['merge']} / ${recon['sum_amount']['partition']}")
    print(f"Row Differences: {recon['differences']}")
