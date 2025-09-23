# Air Audit App (v1)

SPA de tres pantallas con backend FastAPI:
1) Bienvenida (tarjetas de aviones con búsqueda)
2) Ítems (publicados por avión, paginación + búsqueda, editar y agregar)
3) Editar/Nuevo ítem (formulario)

Persistencia append-only en SQLite con bitácora (ledger) y cuarentena para duplicados al importar CSV. Creación/actualización manual permitida con registro de auditoría.

## Run
```bash
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```
Abrir: http://127.0.0.1:8000

## Autenticación

Usuarios demo:

| Rol           | Usuario    | Contraseña    |
|---------------|------------|---------------|
| Administrador | `admin`    | `admin123`    |
| Mecánico      | `mechanic` | `mechanic123` |
| Auditor       | `auditor`  | `auditor123`  |

Permisos:
- Administrador: acceso total (gestión de aviones, importaciones, creación y edición de ítems).
- Mecánico: consulta y creación manual de ítems.
- Auditor: solo lectura (navegación e historial).

## Configuración (variables de entorno)

Opcionales; valores por defecto entre paréntesis:

- `DB_DIR` (`./data`): carpeta para la base SQLite.
- `DB_FILE` (`air_audit.sqlite`): nombre del archivo SQLite.
- `SESSION_DURATION_SECONDS` (`28800`): duración de la sesión (segundos).
- `COOKIE_SECURE` (`false`): si `true`, marca la cookie como Secure.
- `COOKIE_SAMESITE` (`lax`): `lax` | `strict` | `none` (si `none`, se fuerza `Secure=true`).
- `COOKIE_DOMAIN` (vacío): dominio explícito para la cookie.
- `ALLOWED_ORIGINS` (`*`): orígenes permitidos para CORS, CSV. Ej: `http://localhost:3000,https://miapp.com`.
- `CORS_ALLOW_METHODS` (`*`): métodos CORS (CSV) o `*`.
- `CORS_ALLOW_HEADERS` (`*`): headers CORS (CSV) o `*`.
- `CORS_ALLOW_CREDENTIALS` (`true`): habilita credenciales en CORS.

## Notas
- Cookies httpOnly con soporte de sesión vía header `Authorization: Bearer` o cookie `session`.
- CORS debe restringirse en producción estableciendo `ALLOWED_ORIGINS`.
