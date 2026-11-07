import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

def generate_synthetic_batches(seed: int = 42, base_count: int = 5000, delta_count: int = 1000) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    random.seed(seed)
    base_date = datetime(2026, 10, 1, 8, 0, 0)
    batch_1 = []
    for i in range(1, base_count + 1):
        order_date = base_date + timedelta(days=random.randint(0, 4))
        batch_1.append({
            "order_id": f"ORD-{i:07d}", "customer_id": f"CUST-{random.randint(100, 999)}",
            "order_date": order_date.strftime("%Y-%m-%d"), "amount": round(random.uniform(10.0, 500.0), 2),
            "status": "COMPLETED", "updated_at": order_date.strftime("%Y-%m-%d %H:%M:%S")
        })

    batch_2 = []
    update_cnt, insert_cnt = int(delta_count * 0.4), int(delta_count * 0.5)
    for i in range(1, update_cnt + 1):
        u_time = base_date + timedelta(days=random.randint(5, 7))
        batch_2.append({
            "order_id": f"ORD-{i:07d}", "customer_id": f"CUST-{random.randint(100, 999)}",
            "order_date": (base_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            "amount": round(random.uniform(500.0, 1000.0), 2),
            "status": "SHIPPED", "updated_at": u_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    for i in range(base_count + 1, base_count + insert_cnt + 1):
        i_time = base_date + timedelta(days=random.randint(5, 7))
        batch_2.append({
            "order_id": f"ORD-{i:07d}", "customer_id": f"CUST-{random.randint(100, 999)}",
            "order_date": i_time.strftime("%Y-%m-%d"), "amount": round(random.uniform(15.0, 300.0), 2),
            "status": "PENDING", "updated_at": i_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    # Edge cases
    batch_2.append({"order_id": f"ORD-{base_count + insert_cnt + 1:07d}", "customer_id": "CUST-999", "order_date": "2026-09-15", "amount": 199.99, "status": "COMPLETED", "updated_at": "2026-10-08 12:00:00"})
    dup_id = f"ORD-{base_count + insert_cnt + 2:07d}"
    batch_2.append({"order_id": dup_id, "customer_id": "CUST-888", "order_date": "2026-10-06", "amount": 50.0, "status": "PENDING", "updated_at": "2026-10-06 10:00:00"})
    batch_2.append({"order_id": dup_id, "customer_id": "CUST-888", "order_date": "2026-10-06", "amount": 55.0, "status": "COMPLETED", "updated_at": "2026-10-06 10:05:00"})
    batch_2.append({"order_id": None, "customer_id": "CUST-101", "order_date": "2026-10-06", "amount": 10.0, "status": "PENDING", "updated_at": "2026-10-06 10:00:00"})
    batch_2.append({"order_id": "ORD-BAD-001", "customer_id": "CUST-102", "order_date": "bad-date", "amount": 25.0, "status": "PENDING", "updated_at": "2026-10-06 10:00:00"})
    batch_2.append({"order_id": "ORD-BAD-002", "customer_id": "CUST-103", "order_date": "2026-10-06", "amount": -50.0, "status": "PENDING", "updated_at": "2026-10-06 10:00:00"})

    return batch_1, batch_2
