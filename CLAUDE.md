# SD Housing Permits — Development Permit Dashboard

## Project Overview
San Diego development permit analysis. ~1.2M permit records from two permitting systems (legacy 2002-2022, current 2018+). Tracks housing production, solar adoption, and approval timelines for RHNA compliance. Part of SD Civic Data series (#3, after Get It Done 311 and City Budget).

**Repo**: https://github.com/Glenn-Harless/sd-housing-permits

## Architecture

### Project Structure
```
pipeline/       # Data ingestion + transformation (DuckDB)
data/raw/       # Raw source CSVs (gitignored, ~675MB)
data/processed/ # permits.parquet — full 1.2M row dataset (gitignored, 138MB)
data/aggregated/# 9 pre-aggregated parquets — committed to git (~12MB total)
dashboard/      # Streamlit app (5 tabs)
api/            # FastAPI (8 endpoints) + MCP server (8 tools)
```

### Data Flow
```
seshat.datasd.org CSVs → pipeline/ingest.py → data/raw/
data/raw/ → pipeline/transform.py → data/processed/permits.parquet
                                   → data/aggregated/*.parquet (9 files)
data/aggregated/ → dashboard/app.py (Streamlit)
data/aggregated/ → api/queries.py → api/main.py (FastAPI)
                                   → api/mcp_server.py (MCP)
```

### Key Rule: Dashboard and API ONLY query aggregated parquets
Never query `permits.parquet` from dashboard or API code. All queries go through `data/aggregated/`. This keeps the full dataset gitignored and enables deploy without running the pipeline.

## Aggregated Parquets & Filter Columns

| File | Has year | Has type | Has zip | Has source |
|------|----------|----------|---------|------------|
| `permit_volume_monthly` | yes | yes | **NO** | yes |
| `housing_units_by_year` | yes | **NO** | **NO** | **NO** |
| `approval_timelines` | yes | yes | yes | **NO** |
| `solar_permits_monthly` | yes | **NO** | yes | **NO** |
| `map_points` | yes | yes | yes | **NO** |
| `top_permit_types` | **NO** | yes | **NO** | **NO** |
| `construction_by_zip` | yes | **NO** | yes | **NO** |
| `bc_code_summary` | yes | **NO** | **NO** | yes |
| `permit_summary` | yes | yes | yes | yes |

When writing queries, use `has_zip=False` (etc.) in `_where()` for tables missing columns.
`permit_summary` is the universal fallback — has all filter dimensions.

## Key Data Fields
- `approval_type_clean`: Building Permit, Solar/PV, Electrical, Plumbing, Mechanical, Fire, Right of Way, Sign, Other
- `is_housing`: TRUE when bc_code starts with '10' or building permit with DU > 0
- `is_solar`: TRUE for PV/solar permits
- `is_adu`: TRUE for ADU/JADU permits (bc_code 4333 or adu/jadu totals > 0)
- `total_du`: Sum of all dwelling unit fields (DU + ADU + JADU)
- `approval_days`: Calendar days from create to issue (NULL if negative or not issued)

## Data Quirks
- **Zip codes**: Only ~222K/1.2M records have zips (current system only). Must CAST to VARCHAR and use `xaxis_type="category"` in plotly or they render as decimals.
- **Valuation**: NULL for 77% of records (non-building permits)
- **Source overlap**: Legacy and Current systems overlap 2018-2022. Deduped by approval_id.
- **RHNA target**: 108,036 units (6th cycle, 2021-2029)

## Dashboard Rules
- **DuckDB for all data access** — `query()` helper with fresh `duckdb.connect()` per call
- Shared `_where()` for sidebar filters across all tabs
- `requirements.txt` at project root for Streamlit Cloud

## Commands
```
uv run python -m pipeline.build          # Full pipeline (--force to re-download)
uv run streamlit run dashboard/app.py    # Dashboard
uv run uvicorn api.main:app              # FastAPI
uv run python -m api.mcp_server          # MCP server
```

## Deployment
- GitHub Actions weekly refresh (Sunday 8:30 UTC) — rebuilds aggregated parquets
- Streamlit Cloud: uses `requirements.txt`, reads aggregated parquets from repo
- Render: uses `requirements-api.txt` for FastAPI
- Only aggregated parquets committed to git; raw data and permits.parquet are gitignored
