import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

def validate_and_quarantine(records: List[Dict[str, Any]], run_id: str, batch_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    valid_records, quarantined = [], []
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for row in records:
        rejection_reason = None
        raw_payload = json.dumps(row)
        order_id = row.get("order_id")

        if not order_id or str(order_id).strip() == "":
            rejection_reason = "NULL_OR_EMPTY_PRIMARY_KEY"
        elif not row.get("customer_id"):
            rejection_reason = "MISSING_CUSTOMER_ID"
        elif row.get("amount") is None or row.get("amount", 0) <= 0:
            rejection_reason = "INVALID_AMOUNT_VALUE"
        else:
            try:
                datetime.strptime(row.get("order_date", ""), "%Y-%m-%d")
            except (ValueError, TypeError):
                rejection_reason = "MALFORMED_ORDER_DATE"

        if rejection_reason:
            quarantined.append({
                "run_id": run_id, "batch_id": batch_id,
                "order_id": str(order_id) if order_id else "UNKNOWN",
                "raw_payload": raw_payload, "rejection_reason": rejection_reason,
                "rejected_at": now_str
            })
        else:
            clean_row = dict(row)
            clean_row["partition_date"] = clean_row["order_date"]
            clean_row["batch_run_id"] = run_id
            valid_records.append(clean_row)

    deduped = {}
    for r in valid_records:
        oid = r["order_id"]
        if oid not in deduped or r["updated_at"] >= deduped[oid]["updated_at"]:
            deduped[oid] = r

    return list(deduped.values()), quarantined
