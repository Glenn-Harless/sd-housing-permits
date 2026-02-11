# SD Housing Permits — Development Permit Dashboard

## Project Overview
San Diego development permit analysis. ~1.2M permit records from two permitting systems (legacy 2002-2022, current 2018+). Tracks housing production, solar adoption, and approval timelines for RHNA compliance.

## Architecture — Follow sd-city-budget Pattern

### Project Structure
```
pipeline/       # Data ingestion + transformation
data/raw/       # Raw source CSVs (gitignored)
data/processed/ # permits.parquet — full dataset (gitignored, rebuilt by pipeline)
data/aggregated/# 9 pre-aggregated parquets — committed to git, power dashboard/API
dashboard/      # Streamlit app
api/            # FastAPI + MCP server
```

### Key Data Fields
- `approval_type_clean`: Building Permit, Solar/PV, Electrical, Plumbing, Mechanical, Fire, Right of Way, Sign, Other
- `is_housing`: TRUE when bc_code starts with '10' or building permit with DU > 0
- `is_solar`: TRUE for PV/solar permits
- `is_adu`: TRUE for ADU/JADU permits (bc_code 4333 or adu/jadu totals > 0)
- `total_du`: Sum of all dwelling unit fields (DU + ADU + JADU)
- `approval_days`: Calendar days from create to issue (NULL if negative or not issued)

### Dashboard Rules
- **Use DuckDB for all data access** — no Polars/pandas for loading full datasets
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame
- Shared `_where()` for sidebar filters across all tabs
- **Only query aggregated parquets** — dashboard and API never touch permits.parquet
- `requirements.txt` at project root for Streamlit Cloud

### Pipeline
- `uv run python -m pipeline.build` to run full pipeline
- `--force` flag re-downloads all CSVs
- DuckDB for all transforms; parquet output with ZSTD compression

### API
- `api/queries.py` — all SQL lives here (shared by dashboard, FastAPI, MCP)
- `api/main.py` — FastAPI app: `uv run uvicorn api.main:app`
- `api/mcp_server.py` — MCP server: `uv run python -m api.mcp_server`

### Deployment
- GitHub Actions weekly refresh (Sunday 8:30 UTC)
- Only aggregated parquets (~12MB total) committed to git; permits.parquet is gitignored
- Streamlit Cloud and Render deploy from aggregated data only — no pipeline needed at deploy time
