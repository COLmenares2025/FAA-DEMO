import os
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3

from .db import connect, DB_PATH
from .schema_sql import SCHEMA_SQL
from .importer import import_csv_bytes

app = FastAPI(title="Air Audit (Append-only)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        cur = con.cursor()
        cur.executescript(SCHEMA_SQL)
        con.commit()

@app.get("/", response_class=HTMLResponse)
def index():
    path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(path.read_text(encoding="utf-8"))

# Aircraft
@app.post("/aircraft")
def create_aircraft(name: str = Form(...), model: str = Form("N/A")):
    with connect() as con:
        cur = con.cursor()
        cur.execute("INSERT INTO aircraft(name, model) VALUES (?,?)", (name.strip(), (model or 'N/A').strip()))
        con.commit()
        return {"id": cur.lastrowid, "name": name.strip(), "model": (model or 'N/A').strip()}

@app.get("/aircraft")
def list_aircraft():
    with connect() as con:
        rows = con.execute("SELECT id, name, model, created_at FROM aircraft ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]

# Imports
@app.post("/aircraft/{aircraft_id}/imports")
async def upload_import(aircraft_id: int, publish_mode: str = "quarantine", file: UploadFile = File(...)):
    content = await file.read()
    with connect() as con:
        # Verify aircraft
        r = con.execute("SELECT id FROM aircraft WHERE id=?", (aircraft_id,)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Aircraft no encontrado")
        try:
            result = import_csv_bytes(con, aircraft_id, file.filename, content, publish_mode=publish_mode)
            con.commit()
            return result
        except sqlite3.IntegrityError as e:
            con.rollback()
            if "UNIQUE(file_sha256, aircraft_id)" in str(e):
                raise HTTPException(status_code=409, detail="Ese archivo ya fue importado para este avión.")
            raise
        except Exception as e:
            con.rollback()
            raise HTTPException(status_code=400, detail=str(e))

@app.get("/imports/{batch_id}")
def get_import(batch_id: int):
    with connect() as con:
        b = con.execute("SELECT * FROM import_batch WHERE id=?", (batch_id,)).fetchone()
        if not b:
            raise HTTPException(status_code=404, detail="Import batch no encontrado")
        return dict(b)

@app.get("/imports/{batch_id}/errors")
def get_import_errors(batch_id: int, limit: int = 100, offset: int = 0):
    with connect() as con:
        rows = con.execute(
            "SELECT row_index, field, message, severity, created_at FROM import_error WHERE import_batch_id=? ORDER BY id LIMIT ? OFFSET ?",
            (batch_id, limit, offset)
        ).fetchall()
        return [dict(r) for r in rows]

# Items (published)
@app.get("/aircraft/{aircraft_id}/items")
def list_items(aircraft_id: int, limit: int = 100, offset: int = 0):
    with connect() as con:
        rows = con.execute(
            """
            SELECT mi.* FROM v_items_loaded mi
            WHERE mi.aircraft_id=?
            ORDER BY mi.id
            LIMIT ? OFFSET ?
            """, (aircraft_id, limit, offset)
        ).fetchall()
        return [dict(r) for r in rows]

@app.get("/aircraft/{aircraft_id}/items/count")
def count_items(aircraft_id: int):
    with connect() as con:
        row = con.execute(
            """
            SELECT COUNT(*) AS n
            FROM v_items_loaded mi
            WHERE mi.aircraft_id=?
            """, (aircraft_id,)
        ).fetchone()
        return {"aircraft_id": aircraft_id, "count": int(row["n"])}

# Quarantine
@app.get("/aircraft/{aircraft_id}/quarantine")
def list_quarantine(aircraft_id: int, batch_id: int | None = None, limit: int = 100, offset: int = 0):
    with connect() as con:
        if batch_id is None:
            rows = con.execute(
                "SELECT * FROM maintenance_item_quarantine WHERE aircraft_id=? ORDER BY id LIMIT ? OFFSET ?",
                (aircraft_id, limit, offset)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM maintenance_item_quarantine WHERE aircraft_id=? AND import_batch_id=? ORDER BY id LIMIT ? OFFSET ?",
                (aircraft_id, batch_id, limit, offset)
            ).fetchall()
        return [dict(r) for r in rows]

@app.get("/aircraft/{aircraft_id}/quarantine/count")
def count_quarantine(aircraft_id: int, batch_id: int | None = None):
    with connect() as con:
        if batch_id is None:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM maintenance_item_quarantine WHERE aircraft_id=?",
                (aircraft_id,)
            ).fetchone()
        else:
            row = con.execute(
                "SELECT COUNT(*) AS n FROM maintenance_item_quarantine WHERE aircraft_id=? AND import_batch_id=?",
                (aircraft_id, batch_id)
            ).fetchone()
        return {"aircraft_id": aircraft_id, "batch_id": batch_id, "count": int(row["n"])}
