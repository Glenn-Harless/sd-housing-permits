"""Shared query layer — all SQL lives here.

Used by the Streamlit dashboard, FastAPI app, and MCP server.
Each function creates a fresh DuckDB connection, queries parquets, returns list[dict].
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")


def _where(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
    source_system: str | None = None,
    *,
    year_col: str = "year",
    type_col: str = "approval_type_clean",
    zip_col: str = "zip_code",
    source_col: str | None = None,
    has_type: bool = True,
    has_zip: bool = True,
    has_source: bool = False,
) -> str:
    """Build a WHERE clause from optional filter params."""
    clauses: list[str] = []
    if yr_min is not None:
        clauses.append(f"{year_col} >= {int(yr_min)}")
    if yr_max is not None:
        clauses.append(f"{year_col} <= {int(yr_max)}")
    if permit_type and has_type:
        clauses.append(f"{type_col} = '{permit_type.replace(chr(39), chr(39)*2)}'")
    if zip_code and has_zip:
        clauses.append(f"{zip_col} = '{zip_code.replace(chr(39), chr(39)*2)}'")
    if source_system and has_source and source_col:
        clauses.append(f"{source_col} = '{source_system.replace(chr(39), chr(39)*2)}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _q(sql: str) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    con = duckdb.connect()
    df = con.execute(sql).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 1. Filter options ──


def get_filter_options() -> dict:
    """Return available filter values: years, permit types, zip codes, source systems."""
    con = duckdb.connect()
    years = sorted(
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT year FROM '{_AGG}/permit_volume_monthly.parquet' WHERE year IS NOT NULL ORDER BY year"
        ).fetchall()
    )
    types = sorted(
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT approval_type_clean FROM '{_AGG}/top_permit_types.parquet' ORDER BY 1"
        ).fetchall()
    )
    zips = sorted(
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT zip_code FROM '{_AGG}/construction_by_zip.parquet' WHERE zip_code IS NOT NULL ORDER BY 1"
        ).fetchall()
    )
    con.close()
    return {
        "years": years,
        "permit_types": types,
        "zip_codes": zips,
        "source_systems": ["legacy", "current"],
    }


# ── 2. Overview ──


def get_overview(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> dict:
    """Total permits, DUs, valuation, and median approval days."""
    w = _where(yr_min, yr_max, permit_type, zip_code)
    rows = _q(f"""
        SELECT
            SUM(permit_count) AS total_permits,
            SUM(total_du) AS total_du,
            SUM(total_valuation)::BIGINT AS total_valuation,
            CAST(SUM(median_approval_days * count_with_days)
                 / NULLIF(SUM(count_with_days), 0) AS INTEGER) AS median_approval_days
        FROM '{_AGG}/permit_summary.parquet'
        {w}
    """)
    return rows[0] if rows else {}


# ── 3. Permit volume ──


def get_permit_volume(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Monthly permit counts by type."""
    w = _where(yr_min, yr_max, permit_type, zip_code, has_zip=False)
    return _q(f"""
        SELECT year, month, approval_type_clean,
               SUM(permit_count) AS permit_count
        FROM '{_AGG}/permit_volume_monthly.parquet'
        {w}
        GROUP BY year, month, approval_type_clean
        ORDER BY year, month
    """)


# ── 4. Housing units ──


def get_housing_units(
    yr_min: int | None = None,
    yr_max: int | None = None,
) -> list[dict]:
    """Annual dwelling units by income category (for RHNA tracking)."""
    w = _where(yr_min, yr_max)
    return _q(f"""
        SELECT * FROM '{_AGG}/housing_units_by_year.parquet'
        {w}
        ORDER BY year
    """)


# ── 5. Approval timelines ──


def get_approval_timelines(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Median/avg/p90 approval days by type and zip."""
    w = _where(yr_min, yr_max, permit_type, zip_code)
    return _q(f"""
        SELECT year, approval_type_clean, zip_code,
               permit_count, median_days, avg_days, p90_days
        FROM '{_AGG}/approval_timelines.parquet'
        {w}
        ORDER BY year, approval_type_clean
    """)


# ── 6. Solar permits ──


def get_solar_permits(
    yr_min: int | None = None,
    yr_max: int | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Monthly solar permit counts with cumulative totals."""
    w = _where(yr_min, yr_max, zip_code=zip_code, has_type=False)
    return _q(f"""
        SELECT year, month, zip_code, permit_count, cumulative_total
        FROM '{_AGG}/solar_permits_monthly.parquet'
        {w}
        ORDER BY year, month
    """)


# ── 7. Construction by zip ──


def get_construction_by_zip(
    yr_min: int | None = None,
    yr_max: int | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Permit count, total valuation, total DUs by zip code and year."""
    w = _where(yr_min, yr_max, zip_code=zip_code, has_type=False)
    return _q(f"""
        SELECT zip_code, year, permit_count, total_valuation, total_du
        FROM '{_AGG}/construction_by_zip.parquet'
        {w}
        ORDER BY zip_code, year
    """)


# ── 8. Top permit types ──


def get_top_permit_types(
    yr_min: int | None = None,
    yr_max: int | None = None,
) -> list[dict]:
    """Summary stats per permit type: count, avg valuation, median approval days."""
    w = _where(yr_min, yr_max, has_type=False, has_zip=False)
    return _q(f"""
        SELECT
            approval_type_clean,
            SUM(permit_count) AS permit_count,
            (SUM(total_valuation) / NULLIF(SUM(permit_count), 0))::BIGINT AS avg_valuation,
            CAST(SUM(median_approval_days * count_with_days)
                 / NULLIF(SUM(count_with_days), 0) AS INTEGER) AS median_approval_days
        FROM '{_AGG}/permit_summary.parquet'
        {w}
        GROUP BY approval_type_clean
        ORDER BY permit_count DESC
    """)
