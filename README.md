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

## CSV import on aircraft creation

`POST /aircraft` now accepts an optional CSV file using multipart form data. Example:

```bash
curl -F "name=My Plane" \
     -F "model=C-172" \
     -F "csv_file=@items.csv" \
     http://127.0.0.1:8000/aircraft
```

The part named `csv_file` must contain a CSV with the columns used by the importer, such as `Item Code`, `Position`, `Description`, `Type`, `Interval Months`, `Interval Hours`, `Interval Landings`, `Adjusted Interval`, `Part Number`, `Part Serial`, `Last Completed Date`, `Last Completed Hours`, `Last Completed Landings`, `Last Completed City`, `Due Next Date`, `Due Next Hours`, `Due Next Landings`, `Time Remaining`, `Hours Remaining`, `Landings Remaining`, `Status` and `Status Note`.

When provided, the endpoint creates the aircraft and immediately loads the CSV rows. The response includes both the aircraft data and the import result (`import_batch_id`, inserted rows, errors, etc.).
