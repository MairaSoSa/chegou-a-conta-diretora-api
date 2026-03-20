# Chegou a Conta Diretora - API de Dados Educacionais

## Overview
A public FastAPI REST API providing educational data about Brazilian schools. Built with Python/FastAPI and PostgreSQL.

## Architecture
- **Framework**: FastAPI (Python 3.12)
- **Database**: PostgreSQL (via SQLAlchemy + psycopg2)
- **Server**: Uvicorn
- **Port**: 5000

## Project Structure
```
app/
  main.py       # FastAPI app with all route definitions
  db.py         # SQLAlchemy engine setup (reads DATABASE_URL)
database/
  recreate_views.sql   # SQL views (vw_raiox_escola, vw_escola_indicadores, etc.)
ingestion/
  db_config.py         # DB connection helpers for ingestion scripts
  import_*.py          # Data ingestion scripts for each indicator
requirements.txt       # Python dependencies
```

## Database Schema
The API relies on these tables (created via ingestion scripts):
- `escolas` - Base school registry
- `censo_escola_historico` - Census history per school per year
- `inse_escola` - Socioeconomic index (INSE)
- `ideb_escola` - IDEB scores
- `ideb_escola_historico` - Historical IDEB with computed scores
- `afd_escola`, `atu_escola`, `had_escola`, `icg_escola`, `ied_escola`, `ird_escola`, `tdi_escola`, `tnr_escola`, `rendimento_escola` - Various school indicators

Views (defined in `database/recreate_views.sql`):
- `vw_escola_ideb` - Consolidated IDEB view
- `vw_escola_indicadores` - All indicators per school per year
- `vw_raiox_escola` - Latest snapshot per school
- `vw_raiox_escola_publica` - Public schools only
- `vw_escolas_mapa` - Map-ready data
- `vw_ufs`, `vw_municipios`, `vw_ufs_publicas`, `vw_municipios_publicos` - Geographic lookups

## API Endpoints
- `GET /` - Health check
- `GET /documentacao` - Swagger UI
- `GET /guia-api` - ReDoc UI
- `GET /escolas/{co_entidade}` - School snapshot (raio-x)
- `GET /escolas/{co_entidade}/historico` - Full indicator history
- `GET /escolas/{co_entidade}/ideb` - IDEB data
- And many more...

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection string (set automatically by Replit)

## Development Notes
- Tables are created empty; ingestion scripts expect CSV/Excel files in `extracted/` folder
- Views are defined in `database/recreate_views.sql` and must be re-applied after schema changes
- To recreate views: `psql "$DATABASE_URL" -f database/recreate_views.sql`

## Deployment
- Target: autoscale
- Run: `uvicorn app.main:app --host 0.0.0.0 --port 5000`
