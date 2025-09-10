import io, re, sqlite3
from typing import List, Dict, Any, Tuple
import pandas as pd

def strip_or_none(x):
    if pd.isna(x): 
        return None
    s = str(x).strip().replace("\t","")
    return s if s != "" else None

def to_int_or_none(x):
    if pd.isna(x) or x == "": 
        return None
    try:
        if isinstance(x, str):
            x = x.replace(",", "")
        return int(float(x))
    except Exception:
        return None

def parse_hours_remaining(x):
    if x is None: 
        return None
    s = str(x).lower().replace(",", "").strip()
    m = re.search(r"(-?\d+)", s)
    return int(m.group(1)) if m else None

def parse_landings_remaining(x):
    if x is None:
        return None
    s = str(x).lower().replace(",", "").strip()
    m = re.search(r"(-?\d+)", s)
    return int(m.group(1)) if m else None

def parse_adjusted_interval(x):
    if x is None:
        return (None, None, None)
    s = str(x).lower().replace(",", "").strip()
    m = re.match(r"(\d+)\s*(hrs|hr|ldgs|ldg|c)?\s*\(\s*([-+]?\d+)\s*\)", s)
    if not m:
        return (None, None, None)
    val = int(m.group(1))
    unit = m.group(2) or None
    delta = int(m.group(3))
    if unit in ("hr","hrs"):
        unit = "hrs"
    elif unit in ("ldg","ldgs","c"):
        unit = "ldgs"
    return (val, unit, delta)

def parse_time_remaining(x):
    if x is None:
        return (None, None, None)
    s = str(x).lower().strip()
    months = None
    days = None
    overdue = None
    try:
        m = re.search(r"(-?\d+)\s*m", s)
        if m:
            months = int(m.group(1))
        d = re.search(r"(-?\d+)\s*d", s)
        if d:
            days = int(d.group(1))
        overdue = True if (isinstance(months,int) and months < 0) or (isinstance(days,int) and days < 0) else False
    except Exception:
        months, days, overdue = (None, None, None)
    return (months, days, overdue)

def to_date_or_none(x):
    if pd.isna(x) or x is None or str(x).strip() == "":
        return None
    try:
        return pd.to_datetime(x, errors="coerce").date().isoformat()
    except Exception:
        return None

def fingerprint_row(row: dict) -> str:
    import hashlib
    key_fields = [
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
    return hashlib.sha1(("||".join(key_fields)).encode("utf-8")).hexdigest()

EXPECTED_COLS = [
    'Item Code','Position','Description','Type','Interval Months','Interval Hours',
    'Interval Landings','Adjusted Interval','Part Number','Part Serial','Last Completed Date',
    'Last Completed Hours','Last Completed Landings','Last Completed City','Due Next Date',
    'Due Next Hours','Due Next Landings','Time Remaining','Hours Remaining','Landings Remaining',
    'Status','Status Note'
]

ALLOWED_TYPES = {"sbsl","comp","ad","insp"}

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    san = pd.DataFrame()
    san["item_code"] = df["Item Code"].map(strip_or_none)
    san["position"] = df["Position"].map(strip_or_none)
    san["description"] = df["Description"].map(strip_or_none)
    san["type"] = df["Type"].map(lambda x: strip_or_none(x.lower() if not pd.isna(x) else x))
    san["interval_months"] = df["Interval Months"].map(to_int_or_none)
    san["interval_hours"]  = df["Interval Hours"].map(to_int_or_none)
    san["interval_landings"] = df["Interval Landings"].map(to_int_or_none)

    adj = df["Adjusted Interval"].map(strip_or_none).map(parse_adjusted_interval)
    san["adjusted_value"] = [t[0] for t in adj]
    san["adjusted_unit"]  = [t[1] for t in adj]
    san["adjusted_delta"] = [t[2] for t in adj]

    san["part_number"] = df["Part Number"].map(strip_or_none)
    san["part_serial"] = df["Part Serial"].map(strip_or_none)

    san["last_completed_date"]     = df["Last Completed Date"].map(strip_or_none).map(to_date_or_none)
    san["last_completed_hours"]    = df["Last Completed Hours"].map(to_int_or_none)
    san["last_completed_landings"] = df["Last Completed Landings"].map(to_int_or_none)
    san["last_completed_city"]     = df["Last Completed City"].map(strip_or_none)

    san["due_next_date"]     = df["Due Next Date"].map(strip_or_none).map(to_date_or_none)
    san["due_next_hours"]    = df["Due Next Hours"].map(to_int_or_none)
    san["due_next_landings"] = df["Due Next Landings"].map(to_int_or_none)

    san["time_remaining_text"] = df["Time Remaining"].map(strip_or_none)
    tm = san["time_remaining_text"].map(parse_time_remaining)
    san["months_remaining"] = [t[0] for t in tm]
    san["days_remaining"]   = [t[1] for t in tm]
    san["is_overdue_time"]  = [t[2] for t in tm]

    san["hours_remaining"]    = df["Hours Remaining"].map(strip_or_none).map(parse_hours_remaining)
    san["landings_remaining"] = df["Landings Remaining"].map(strip_or_none).map(parse_landings_remaining)

    san["status"] = df["Status"].map(strip_or_none)
    san["status_note"] = df["Status Note"].map(strip_or_none)

    san["fingerprint"] = san.apply(lambda r: fingerprint_row(r.to_dict()), axis=1)
    return san

def validate_rows(san: pd.DataFrame) -> List[Dict[str, Any]]:
    errors: List[Dict[str, Any]] = []
    def add_error(i, field, msg, severity="error"):
        errors.append({"row_index": int(i), "field": field, "message": msg, "severity": severity})
    for i, row in san.iterrows():
        if not row["description"]:
            add_error(i, "description", "Description es requerida.")
        for field in ["interval_months","interval_hours","interval_landings",
                      "last_completed_hours","last_completed_landings",
                      "due_next_hours","due_next_landings",
                      "hours_remaining","landings_remaining",
                      "adjusted_value"]:
            v = row[field]
            if v is not None and v < 0:
                add_error(i, field, "Debe ser >= 0.")
        if row["type"] is not None and row["type"] not in ALLOWED_TYPES:
            add_error(i, "type", f"Tipo inválido '{row['type']}'.", "warning")
        lcd = row["last_completed_date"]
        dnd = row["due_next_date"]
        if lcd and dnd and dnd < lcd:
            add_error(i, "due_next_date", "Due Next Date anterior a Last Completed Date.", "warning")
    return errors

def import_csv_bytes(con: sqlite3.Connection, aircraft_id: int, file_name: str, content: bytes, publish_mode: str = "quarantine") -> Dict[str, Any]:
    df = pd.read_csv(io.BytesIO(content))
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas esperadas: {missing}")
    san = sanitize_dataframe(df)
    errors = validate_rows(san)
    total_rows = len(san)

    from .utils import sha256_bytes
    sha = sha256_bytes(content)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("INSERT INTO import_batch(aircraft_id, file_name, file_sha256, total_rows, status) VALUES (?, ?, ?, ?, 'uploaded')",
                (aircraft_id, file_name, sha, total_rows))
    batch_id = cur.lastrowid
    cur.execute("UPDATE import_batch SET status='validated' WHERE id=?", (batch_id,))

    if errors:
        cur.executemany(
            "INSERT INTO import_error(import_batch_id, row_index, field, message, severity) VALUES (?,?,?,?,?)",
            [(batch_id, e["row_index"], e["field"], e["message"], e["severity"]) for e in errors]
        )

    inserted = 0
    hard_error_rows = set(e["row_index"] for e in errors if e["severity"] == "error" and e["field"] != "INSERT")
    for i, row in san.iterrows():
        if i in hard_error_rows: continue
        rec = (
            aircraft_id, batch_id,
            row["item_code"], row["position"], row["description"], row["type"],
            row["interval_months"], row["interval_hours"], row["interval_landings"],
            row["adjusted_value"], row["adjusted_unit"], row["adjusted_delta"],
            row["part_number"], row["part_serial"],
            row["last_completed_date"], row["last_completed_hours"], row["last_completed_landings"], row["last_completed_city"],
            row["due_next_date"], row["due_next_hours"], row["due_next_landings"],
            row["time_remaining_text"], row["months_remaining"], row["days_remaining"], int(row["is_overdue_time"]) if isinstance(row["is_overdue_time"], bool) else row["is_overdue_time"],
            row["hours_remaining"], row["landings_remaining"], row["status"], row["status_note"], row["fingerprint"]
        )
        try:
            cur.execute("""
            INSERT INTO maintenance_item(
                aircraft_id, import_batch_id, item_code, position, description, type,
                interval_months, interval_hours, interval_landings,
                adjusted_value, adjusted_unit, adjusted_delta,
                part_number, part_serial,
                last_completed_date, last_completed_hours, last_completed_landings, last_completed_city,
                due_next_date, due_next_hours, due_next_landings,
                time_remaining_text, months_remaining, days_remaining, is_overdue_time,
                hours_remaining, landings_remaining, status, status_note, fingerprint
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, rec)
            inserted += 1
        except sqlite3.IntegrityError as ex:
            cur.execute(
                "INSERT INTO import_error(import_batch_id, row_index, field, message, severity) VALUES (?,?,?,?, 'error')",
                (batch_id, int(i), "INSERT", str(ex))
            )

    quarantined = 0
    if publish_mode == "quarantine":
        rows = cur.execute("""
            SELECT row_index, message FROM import_error 
            WHERE import_batch_id=? AND severity='error' AND field='INSERT'
        """, (batch_id,)).fetchall()
        err_map = {int(r[0]): r[1] for r in rows}
        ins_sql = """
        INSERT OR IGNORE INTO maintenance_item_quarantine (
            aircraft_id, import_batch_id, source_row_index, reason, error_message,
            item_code, position, description, type, interval_months, interval_hours, interval_landings,
            adjusted_value, adjusted_unit, adjusted_delta, part_number, part_serial,
            last_completed_date, last_completed_hours, last_completed_landings, last_completed_city,
            due_next_date, due_next_hours, due_next_landings, time_remaining_text, months_remaining, days_remaining,
            is_overdue_time, hours_remaining, landings_remaining, status, status_note, fingerprint
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        for i in err_map.keys():
            r = san.iloc[i].to_dict()
            cur.execute(ins_sql, (
                aircraft_id, batch_id, int(i), "duplicate_in_batch", err_map[i],
                r["item_code"], r["position"], r["description"], r["type"],
                r["interval_months"], r["interval_hours"], r["interval_landings"],
                r["adjusted_value"], r["adjusted_unit"], r["adjusted_delta"],
                r["part_number"], r["part_serial"],
                r["last_completed_date"], r["last_completed_hours"], r["last_completed_landings"], r["last_completed_city"],
                r["due_next_date"], r["due_next_hours"], r["due_next_landings"],
                r["time_remaining_text"], r["months_remaining"], r["days_remaining"],
                int(r["is_overdue_time"]) if isinstance(r["is_overdue_time"], bool) else r["is_overdue_time"],
                r["hours_remaining"], r["landings_remaining"], r["status"], r["status_note"], r["fingerprint"]
            ))
            quarantined += 1
        status = 'loaded'
    else:
        status = 'failed' if cur.execute("SELECT COUNT(*) FROM import_error WHERE import_batch_id=? AND severity='error'", (batch_id,)).fetchone()[0] > 0 else 'loaded'

    error_count = cur.execute("SELECT COUNT(*) FROM import_error WHERE import_batch_id=?", (batch_id,)).fetchone()[0]
    cur.execute("""
        UPDATE import_batch
        SET inserted_rows=?, error_rows=?, status=?, completed_at=datetime('now')
        WHERE id=?
    """, (inserted, error_count, status, batch_id))

    return {
        "import_batch_id": int(batch_id),
        "inserted_rows": int(inserted),
        "total_rows": int(total_rows),
        "errors": int(error_count),
        "status": status,
        "quarantined": int(quarantined)
    }
