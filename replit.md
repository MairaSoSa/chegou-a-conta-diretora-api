# Chegou a Conta Diretora - API de Dados Educacionais

## Overview
Public FastAPI REST API providing Brazilian school educational data (Censo Escolar, IDEB, INSE, AFD, and other INEP indicators). Built with Python/FastAPI and PostgreSQL.

## Architecture
- **Framework**: FastAPI (Python 3.12)
- **Database**: Railway PostgreSQL (external) — accessed via `RAILWAY_DATABASE_URL` env var
- **Server**: Uvicorn
- **Port**: 5000

## Project Structure
```
app/
  main.py       # FastAPI app with all route definitions (~2700 lines, 40+ endpoints)
  db.py         # SQLAlchemy engine setup — reads RAILWAY_DATABASE_URL → DATABASE_URL → DATABASE_PUBLIC_URL
database/
  recreate_views.sql        # SQL views
  migration_endereco.sql    # Adds address columns to censo_escola_historico
ingestion/
  db_config.py              # DB connection helpers for ingestion scripts
  import_censo_completo.py  # Main Censo Escolar import (all types, streaming for escola)
  import_censo_historico.py # Legacy import script
requirements.txt            # Python dependencies
extracted/
  2024/
    microdados_ed_basica_2024.csv  # 218MB, 215,545 rows, 426 columns (downloaded from INEP)
```

## Database — Railway PostgreSQL
Connection: `postgresql://postgres:fGOMFFDHZRqNDuMQdBUAgtUEQpuurxlv@hopper.proxy.rlwy.net:24550/railway`
Set via env var: `RAILWAY_DATABASE_URL`

### Tables
- `censo_escola_historico` - Census history per school per year (215,545 rows of 2024 data with full 426-column JSON + address fields)
- `inse_escola` - Socioeconomic index (2021–2023)
- `ideb_escola`, `ideb_escola_historico` - IDEB scores
- `afd_escola`, `atu_escola`, `had_escola`, `icg_escola`, `ied_escola`, `ird_escola`, `tdi_escola`, `tnr_escola`, `rendimento_escola` - Various school indicators
- `matricula_escola`, `docente_escola`, `turma_escola`, `gestor_escolar`, `curso_tecnico_escola` - 2025 data only

### censo_escola_historico schema (key columns)
- `ano`, `co_entidade` (PK)
- `no_entidade`, `co_municipio`, `no_municipio`, `sg_uf`
- `tp_dependencia`, `tp_localizacao`, `tp_situacao_funcionamento`
- `ds_endereco`, `no_bairro`, `co_cep`, `nu_ddd`, `nu_telefone` (address — added in migration_endereco.sql)
- `dados_json` JSONB — full 426-column census record

### Data status
- **2024**: 215,545 schools — full 426 columns + address fields ✅
- **2020–2023**: exists but only ~10 fields in dados_json (old import script) — needs reimport
- **2025**: from Tabela_Escola_2025.csv — structured fields only (no dados_json)

## API Endpoints (40+)
- `GET /` — Health check
- `GET /documentacao` — Swagger UI
- `GET /guia-api` — ReDoc UI
- `GET /censo/{co_entidade}/{ano}` — Full census data for a school in a year (438 fields for 2024)
- `GET /censo/{co_entidade}` — All years of census data for a school
- `GET /escolas/{co_entidade}` — School snapshot (raio-x)
- `GET /escolas/{co_entidade}/historico` — Full indicator history
- `GET /matricula/{co_entidade}` — Enrollment data
- `GET /gestor/{co_entidade}` — School manager data
- `GET /docentes/{co_entidade}` — Teacher data
- `GET /turmas/{co_entidade}` — Class data
- `GET /inse/{co_entidade}` — Socioeconomic index
- `GET /atu/{co_entidade}`, `/had`, `/ied`, `/tdi`, `/ird`, `/icg`, `/tnr` — Various indicators
- `GET /rendimento/{co_entidade}` — School performance data
- And many more geographic/aggregate endpoints

## Environment Variables
- `RAILWAY_DATABASE_URL` — Railway PostgreSQL URL (takes precedence)
- `DATABASE_URL` — Replit managed local PostgreSQL (fallback)

## Ingestion Scripts
### import_censo_completo.py
- Auto-detects file type from filename (escola/matricula/docente/turma/gestor/curso_tecnico)
- Escola files: uses streaming chunk reader (10K rows/chunk) to handle 218MB+ files
- Other types: reads full DataFrame (smaller files)
- Usage: `DATABASE_URL="postgresql://..." python3 ingestion/import_censo_completo.py`
- Expects CSV files in `extracted/` directory (any subdirectory)

## Key Technical Notes
- `importar_escola_streaming()` reads CSV in 10K-row chunks to avoid OOM with 218MB files
- Each chunk: ~3s JSON building (itertuples) + ~30s Railway insert = ~33s/10K rows
- NaN values in CSV are replaced with null (JSON) before serialization
- Address columns migrated via `database/migration_endereco.sql`
- `gdown` installed for downloading from Google Drive (MEC data)

## Deployment
- Target: autoscale
- Run: `uvicorn app.main:app --host 0.0.0.0 --port 5000`
