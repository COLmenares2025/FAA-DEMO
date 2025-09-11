# Air Audit App (v1 screens)

Three-screen SPA with FastAPI backend:
1) Welcome (aircraft cards with search)
2) Items (published items per aircraft, paginated + search, edit button and "add" button)
3) Edit/New item (form)

Append-only persistence with SQLite; quarantine for duplicates on CSV import. Manual create/update allowed with ledger audit entries.

## Run
```bash
cd air-audit-app-v1
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
Open http://127.0.0.1:8000
