import os
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3

from .db import connect, DB_PATH
from .schema_sql import SCHEMA_SQL
from .utils import normalize_payload, fingerprint_from_row, diff_rows
from .importer import import_csv_bytes

app = FastAPI(title="Air Audit (Append-only) — v1 screens")

# CORS (adjust in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    # Ensure schema
    with connect() as con:
        cur = con.cursor()
        cur.executescript(SCHEMA_SQL)
        con.commit()

@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# ---------------------- Aircraft ----------------------
@app.post("/aircraft")
async def create_aircraft(
    name: str = Form(...),
    model: str = Form("N/A"),
    csv_file: UploadFile | None = File(None)
):
    name = name.strip()
    model = (model or "N/A").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Nombre requerido")
    with connect() as con:
        cur = con.cursor()
        cur.execute("INSERT INTO aircraft(name, model) VALUES (?,?)", (name, model))
        aircraft_id = cur.lastrowid
        import_result = None
        if csv_file is not None:
            try:
                content = await csv_file.read()
                import_result = import_csv_bytes(con, aircraft_id, csv_file.filename, content)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        con.commit()
        return {"aircraft": {"id": aircraft_id, "name": name, "model": model}, "import_result": import_result}

@app.get("/aircraft")
def list_aircraft():
    with connect() as con:
        rows = con.execute("SELECT id, name, model, created_at FROM aircraft ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

# ---------------------- Items (published) ----------------------
@app.get("/aircraft/{aircraft_id}/items")
def list_items(aircraft_id: int, limit: int = 50, offset: int = 0, search: Optional[str] = None):
    q = """
        SELECT mi.*
        FROM v_items_loaded mi
        WHERE mi.aircraft_id = :aid
    """
    params = {"aid": aircraft_id, "limit": limit, "offset": offset}
    if search:
        q += " AND (mi.description LIKE :kw OR mi.item_code LIKE :kw)"
        params["kw"] = f"%{search}%"
    q += " ORDER BY mi.id LIMIT :limit OFFSET :offset"
    with connect() as con:
        rows = con.execute(q, params).fetchall()
        return [dict(r) for r in rows]

@app.get("/aircraft/{aircraft_id}/items/count")
def count_items(aircraft_id: int, search: Optional[str] = None):
    q = """
        SELECT COUNT(*) AS n
        FROM v_items_loaded mi
        WHERE mi.aircraft_id = :aid
    """
    params = {"aid": aircraft_id}
    if search:
        q += " AND (mi.description LIKE :kw OR mi.item_code LIKE :kw)"
        params["kw"] = f"%{search}%"
    with connect() as con:
        n = con.execute(q, params).fetchone()["n"]
        return {"count": int(n)}

# ---------------------- Item detail & update ----------------------
@app.get("/items/{item_id}")
def get_item(item_id: int):
    with connect() as con:
        r = con.execute("SELECT * FROM maintenance_item WHERE id=?", (item_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Item no encontrado")
        return dict(r)

@app.put("/items/{item_id}")
def update_item(item_id: int, payload: Dict[str, Any]):
    # Normalize input
    update = normalize_payload(payload)
    if not update:
        raise HTTPException(status_code=400, detail="Sin cambios")
    # Fetch current row
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("SELECT * FROM maintenance_item WHERE id=?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Item no encontrado")
        current = dict(row)
        # Merge values
        new_values = current.copy()
        new_values.update(update)
        # Recompute fingerprint (may change if key fields change)
        new_values["fingerprint"] = fingerprint_from_row(new_values)
        # Build dynamic SQL
        sets = [f"{k}=:{k}" for k in update.keys()]
        sets.append("fingerprint=:fingerprint")
        sql = "UPDATE maintenance_item SET " + ", ".join(sets) + " WHERE id=:id"
        try:
            cur.execute(sql, {**new_values, "id": item_id})
        except sqlite3.IntegrityError as e:
            # Likely UNIQUE(import_batch_id, fingerprint) conflict
            raise HTTPException(status_code=409, detail=f"Conflicto de duplicado dentro del lote: {e}")
        # Ledger
        changed = diff_rows(current, new_values)
        if changed:
            details = {"action":"UPDATE","table":"maintenance_item","item_id": item_id,"diff": changed}
            cur.execute("INSERT INTO data_ledger(table_name, action, row_id, import_batch_id, details) VALUES (?,?,?,?,?)",
                        ("maintenance_item","UPDATE", item_id, current["import_batch_id"], json_dumps(details)))
        con.commit()
        return {"id": item_id, "updated_fields": list(update.keys())}

# ---------------------- Create manual item ----------------------
def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)

@app.post("/aircraft/{aircraft_id}/items")
def create_item(aircraft_id: int, payload: Dict[str, Any]):
    data = normalize_payload(payload)
    # Required
    if not data.get("description"):
        raise HTTPException(status_code=400, detail="description es requerido")
    # Find or create a manual import batch
    manual_sig = f"manual::{aircraft_id}"
    with connect() as con:
        cur = con.cursor()
        # verify aircraft exists
        a = cur.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not a:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        b = cur.execute("SELECT id FROM import_batch WHERE aircraft_id=? AND file_sha256=?", (aircraft_id, manual_sig)).fetchone()
        if not b:
            cur.execute("""
                INSERT INTO import_batch(aircraft_id, file_name, file_sha256, total_rows, inserted_rows, error_rows, status)
                VALUES (?, 'manual-api', ?, 0, 0, 0, 'loaded')
            """, (aircraft_id, manual_sig))
            batch_id = cur.lastrowid
        else:
            batch_id = b["id"]
        # Prepare insert
        data_full = {
            "aircraft_id": aircraft_id,
            "import_batch_id": batch_id,
            **data
        }
        data_full["fingerprint"] = fingerprint_from_row(data_full)
        cols = ",".join(data_full.keys())
        vals = ":" + ",:".join(data_full.keys())
        sql = f"INSERT INTO maintenance_item({cols}) VALUES ({vals})"
        try:
            cur.execute(sql, data_full)
        except sqlite3.IntegrityError as e:
            # Duplicate inside manual batch
            raise HTTPException(status_code=409, detail=f"Ítem duplicado dentro del lote manual: {e}")
        # Update batch counters (optional)
        cur.execute("UPDATE import_batch SET inserted_rows = inserted_rows + 1 WHERE id=?", (batch_id,))
        # Ledger
        details = {"action":"INSERT","table":"maintenance_item","item_id": cur.lastrowid,"source":"manual","values": data}
        cur.execute("INSERT INTO data_ledger(table_name, action, row_id, import_batch_id, details) VALUES (?,?,?,?,?)",
                    ("maintenance_item","INSERT", cur.lastrowid, batch_id, json_dumps(details)))
        con.commit()
        return {"id": cur.lastrowid, "import_batch_id": batch_id}
