# Air Audit App (Append-only) — con paginación

## Puesta en marcha
```bash
cd air-audit-app
python -m venv .venv
# Windows: .venv\Scripts\Activate ; macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
- UI: http://127.0.0.1:8000/
- API: http://127.0.0.1:8000/docs

## Endpoints clave
- `POST /aircraft`
- `GET  /aircraft`
- `POST /aircraft/{aircraft_id}/imports?publish_mode=quarantine`
- `GET  /aircraft/{aircraft_id}/items?limit=&offset=`
- `GET  /aircraft/{aircraft_id}/items/count`
- `GET  /aircraft/{aircraft_id}/quarantine?limit=&offset=`
- `GET  /aircraft/{aircraft_id}/quarantine/count`
- `GET  /imports/{batch_id}`
- `GET  /imports/{batch_id}/errors`

## Notas
- Persistencia **append-only** con triggers anti-DELETE.
- Opción B: duplicados -> **cuarentena** y el lote queda `loaded`.
- UI con **paginación** (50/100/250/500) en Publicados y Cuarentena.
