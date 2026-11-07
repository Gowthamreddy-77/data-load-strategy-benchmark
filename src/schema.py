SOURCE_COLUMNS = ["order_id", "customer_id", "order_date", "amount", "status", "updated_at"]
TARGET_COLUMNS = ["order_id", "customer_id", "order_date", "partition_date", "amount", "status", "updated_at", "batch_run_id"]

DDL_STATEMENTS = {
    "audit_log": """
        CREATE TABLE IF NOT EXISTS meta_audit_log (
            run_id VARCHAR, strategy VARCHAR, batch_id VARCHAR,
            start_time TIMESTAMP, end_time TIMESTAMP, duration_ms DOUBLE,
            input_count BIGINT, valid_count BIGINT, rejected_count BIGINT,
            target_count_after BIGINT, status VARCHAR, error_message VARCHAR
        );
    """,
    "quarantine": """
        CREATE TABLE IF NOT EXISTS raw_quarantine (
            run_id VARCHAR, batch_id VARCHAR, order_id VARCHAR,
            raw_payload VARCHAR, rejection_reason VARCHAR, rejected_at TIMESTAMP
        );
    """,
    "target_full": """
        CREATE TABLE IF NOT EXISTS target_orders_full (
            order_id VARCHAR PRIMARY KEY, customer_id VARCHAR NOT NULL,
            order_date DATE NOT NULL, partition_date DATE NOT NULL,
            amount DOUBLE NOT NULL, status VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL, batch_run_id VARCHAR NOT NULL
        );
    """,
    "target_merge": """
        CREATE TABLE IF NOT EXISTS target_orders_merge (
            order_id VARCHAR PRIMARY KEY, customer_id VARCHAR NOT NULL,
            order_date DATE NOT NULL, partition_date DATE NOT NULL,
            amount DOUBLE NOT NULL, status VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL, batch_run_id VARCHAR NOT NULL
        );
    """,
    "target_partition": """
        CREATE TABLE IF NOT EXISTS target_orders_partition (
            order_id VARCHAR, customer_id VARCHAR NOT NULL,
            order_date DATE NOT NULL, partition_date DATE NOT NULL,
            amount DOUBLE NOT NULL, status VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL, batch_run_id VARCHAR NOT NULL,
            PRIMARY KEY (partition_date, order_id)
        );
    """
}
