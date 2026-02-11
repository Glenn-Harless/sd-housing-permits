"""MCP server for San Diego housing permits data.

Exposes 8 tools that let Claude query permit parquet files directly.
Uses FastMCP (v2) with stdio transport — spawned by Claude Code as a subprocess.
"""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego Housing Permits",
    instructions=(
        "San Diego development permit data covering 2002–present from two permitting "
        "systems (legacy 2002-2022, current 2018+). Includes ~1.2M permit records with "
        "dwelling unit counts by income category, approval timelines, solar permits, "
        "and geo data. Call get_filter_options first to see available years, permit "
        "types, and zip codes. Amounts in valuation are US dollars."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values.

    Returns years (int), permit_types (str), zip_codes (str),
    and source_systems (str). Call this first to discover valid filter values.
    """
    return queries.get_filter_options()


@mcp.tool()
def get_overview(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> dict:
    """Get summary stats: total permits, dwelling units, valuation (USD), median approval days.

    Filter by year range, permit type (e.g. 'Building Permit', 'Solar/PV'),
    and/or zip code.
    """
    return queries.get_overview(yr_min, yr_max, permit_type, zip_code)


@mcp.tool()
def get_permit_volume(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Get monthly permit counts by approval type.

    Returns year, month, approval_type_clean, permit_count.
    """
    return queries.get_permit_volume(yr_min, yr_max, permit_type, zip_code)


@mcp.tool()
def get_housing_units(
    yr_min: int | None = None,
    yr_max: int | None = None,
) -> list[dict]:
    """Get annual dwelling units by income category for RHNA tracking.

    Returns year plus DU counts: du_extremely_low, du_very_low, du_low,
    du_moderate, du_above_moderate, adu_total, jadu_total, total_du.
    Only includes housing permits (is_housing=true).
    """
    return queries.get_housing_units(yr_min, yr_max)


@mcp.tool()
def get_approval_timelines(
    yr_min: int | None = None,
    yr_max: int | None = None,
    permit_type: str | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Get approval day statistics by permit type and zip code.

    Returns year, approval_type_clean, zip_code, permit_count,
    median_days, avg_days, p90_days. Days = calendar days from
    approval create to approval issue.
    """
    return queries.get_approval_timelines(yr_min, yr_max, permit_type, zip_code)


@mcp.tool()
def get_solar_permits(
    yr_min: int | None = None,
    yr_max: int | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Get monthly solar/PV permit counts with cumulative totals.

    Returns year, month, zip_code, permit_count, cumulative_total.
    """
    return queries.get_solar_permits(yr_min, yr_max, zip_code)


@mcp.tool()
def get_construction_by_zip(
    yr_min: int | None = None,
    yr_max: int | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Get construction activity by zip code: permits, valuation (USD), dwelling units.

    Returns zip_code, year, permit_count, total_valuation, total_du.
    """
    return queries.get_construction_by_zip(yr_min, yr_max, zip_code)


@mcp.tool()
def get_top_permit_types(
    yr_min: int | None = None,
    yr_max: int | None = None,
) -> list[dict]:
    """Get summary stats per permit type: count, avg valuation (USD), median approval days.

    Returns approval_type_clean, permit_count, avg_valuation, median_approval_days.
    """
    return queries.get_top_permit_types(yr_min, yr_max)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
