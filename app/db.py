import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_DIR = Path(os.getenv("DB_DIR", "./data"))
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / os.getenv("DB_FILE", "air_audit.sqlite")

def get_connection():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

@contextmanager
def connect():
    con = get_connection()
    try:
        yield con
    finally:
        con.close()
