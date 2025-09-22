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

## Autenticación

Al iniciar la aplicación, ingresa con alguno de los usuarios demo:

| Rol          | Usuario   | Contraseña    |
|--------------|-----------|---------------|
| Administrador| `admin`   | `admin123`    |
| Mecánico     | `mechanic`| `mechanic123` |
| Auditor      | `auditor` | `auditor123`  |

Los roles tienen los siguientes permisos:

- **Administrador:** acceso total (gestión de aviones, importaciones, creación y edición de ítems).
- **Mecánico:** puede consultar información y crear ítems manuales.
- **Auditor:** acceso de solo lectura para navegar por la información y el historial.
