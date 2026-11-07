import pytest
from src.pipeline import IngestionEngine

@pytest.fixture
def engine():
    return IngestionEngine(":memory:")

def test_happy_path_and_equivalence(engine):
    b1 = [{"order_id": "1", "customer_id": "C1", "order_date": "2026-10-01", "amount": 100.0, "status": "COMPLETED", "updated_at": "2026-10-01 10:00:00"}]
    b2 = [
        {"order_id": "1", "customer_id": "C1", "order_date": "2026-10-01", "amount": 150.0, "status": "COMPLETED", "updated_at": "2026-10-02 10:00:00"},
        {"order_id": "2", "customer_id": "C2", "order_date": "2026-10-02", "amount": 200.0, "status": "PENDING", "updated_at": "2026-10-02 10:00:00"}
    ]
    engine.load_full_refresh(b1, "B1")
    engine.load_full_refresh(b1 + b2, "B2")
    engine.load_incremental_merge(b1, "B1")
    engine.load_incremental_merge(b2, "B2")
    engine.load_partition_replacement(b1, "B1")
    engine.load_partition_replacement(b2, "B2")

    f = engine.con.execute("SELECT COUNT(*) FROM target_orders_full").fetchone()[0]
    m = engine.con.execute("SELECT COUNT(*) FROM target_orders_merge").fetchone()[0]
    p = engine.con.execute("SELECT COUNT(*) FROM target_orders_partition").fetchone()[0]
    assert f == 2 and m == 2 and p == 2

def test_malformed_quarantined(engine):
    dirty = [
        {"order_id": None, "customer_id": "C1", "order_date": "2026-10-01", "amount": 10.0, "status": "P", "updated_at": "2026-10-01 10:00:00"},
        {"order_id": "2", "customer_id": "C2", "order_date": "bad-date", "amount": 20.0, "status": "P", "updated_at": "2026-10-01 10:00:00"},
        {"order_id": "3", "customer_id": "C3", "order_date": "2026-10-01", "amount": -5.0, "status": "P", "updated_at": "2026-10-01 10:00:00"},
        {"order_id": "4", "customer_id": "C4", "order_date": "2026-10-01", "amount": 40.0, "status": "P", "updated_at": "2026-10-01 10:00:00"}
    ]
    engine.load_incremental_merge(dirty, "B_DIRTY")
    assert engine.con.execute("SELECT COUNT(*) FROM target_orders_merge").fetchone()[0] == 1
    assert engine.con.execute("SELECT COUNT(*) FROM raw_quarantine").fetchone()[0] == 3

def test_idempotent_rerun(engine):
    b = [{"order_id": "R1", "customer_id": "C1", "order_date": "2026-10-01", "amount": 50.0, "status": "OK", "updated_at": "2026-10-01 10:00:00"}]
    engine.load_incremental_merge(b, "B_IDEM")
    engine.load_incremental_merge(b, "B_IDEM")
    assert engine.con.execute("SELECT COUNT(*) FROM target_orders_merge").fetchone()[0] == 1
