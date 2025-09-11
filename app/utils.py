import hashlib
from datetime import datetime
from typing import Optional, Dict, Any

def sha256_bytes(b: bytes) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def strip_or_none(x: Optional[str]):
    if x is None:
        return None
    s = str(x).strip().replace("\t", "")
    return s if s else None

def to_int_or_none(x):
    if x is None or x == "":
        return None
    try:
        if isinstance(x, str):
            x = x.replace(",", "")
        return int(float(x))
    except Exception:
        return None

def to_date_iso(x: Optional[str]):
    if x is None or str(x).strip() == "":
        return None
    try:
        import pandas as pd
        return pd.to_datetime(x, errors="coerce").date().isoformat()
    except Exception:
        return None

ALLOWED_FIELDS = {
    "item_code","position","description","type",
    "interval_months","interval_hours","interval_landings",
    "adjusted_value","adjusted_unit","adjusted_delta",
    "part_number","part_serial",
    "last_completed_date","last_completed_hours","last_completed_landings","last_completed_city",
    "due_next_date","due_next_hours","due_next_landings",
    "time_remaining_text","months_remaining","days_remaining","is_overdue_time",
    "hours_remaining","landings_remaining",
    "status","status_note"
}

def normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    # keep only known fields, normalize types
    out = {}
    for k,v in payload.items():
        if k not in ALLOWED_FIELDS:
            continue
        if k in {"item_code","position","description","type","part_number","part_serial","last_completed_city","status","status_note","time_remaining_text","adjusted_unit"}:
            out[k] = strip_or_none(v)
        elif k in {"interval_months","interval_hours","interval_landings","adjusted_value","adjusted_delta","last_completed_hours","last_completed_landings","due_next_hours","due_next_landings","months_remaining","days_remaining","hours_remaining","landings_remaining"}:
            out[k] = to_int_or_none(v)
        elif k in {"is_overdue_time"}:
            if v in (True, False, 0,1,"0","1"):
                out[k] = int(v) if not isinstance(v,bool) else int(v)
            else:
                out[k] = None
        elif k in {"last_completed_date","due_next_date"}:
            out[k] = to_date_iso(v)
        else:
            out[k] = v
    # normalize type to lowercase if present
    if "type" in out and isinstance(out["type"], str):
        out["type"] = out["type"].lower()
    return out

def fingerprint_from_row(row: Dict[str, Any]) -> str:
    parts = [
        row.get("item_code") or "",
        row.get("position") or "",
        row.get("description") or "",
        row.get("type") or "",
        row.get("part_number") or "",
        row.get("part_serial") or "",
        str(row.get("interval_months") or ""),
        str(row.get("interval_hours") or ""),
        str(row.get("interval_landings") or ""),
    ]
    return hashlib.sha1(("||".join(parts)).encode("utf-8")).hexdigest()

def diff_rows(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    changed = {}
    keys = set(before.keys()) | set(after.keys())
    for k in keys:
        if before.get(k) != after.get(k):
            changed[k] = {"from": before.get(k), "to": after.get(k)}
    return changed
