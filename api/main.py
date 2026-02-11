"""FastAPI app — thin wrappers around the shared query layer."""

from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from api import queries
from api.models import (
    ApprovalTimeline,
    ConstructionByZip,
    FilterOptions,
    HousingUnits,
    OverviewResponse,
    PermitTypeSummary,
    PermitVolume,
    SolarPermit,
)

app = FastAPI(
    title="San Diego Housing Permits API",
    description=(
        "Query San Diego's development permit data: permit volume, housing units, "
        "approval timelines, solar adoption, and construction by zip code. "
        "Data covers 2002–present from two permitting systems (legacy + current)."
    ),
    version="0.1.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
def health():
    """Debug endpoint — shows data path and file availability."""
    from pathlib import Path

    agg = Path(queries._AGG)
    files = sorted(p.name for p in agg.glob("*.parquet")) if agg.exists() else []
    return {"agg_path": str(agg), "exists": agg.exists(), "files": files}


@app.get("/")
def root():
    return {
        "message": "San Diego Housing Permits API",
        "docs": "/docs",
        "endpoints": [
            "/filters",
            "/overview",
            "/permit-volume",
            "/housing-units",
            "/approval-timelines",
            "/solar-permits",
            "/construction-by-zip",
            "/top-permit-types",
        ],
    }


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available years, permit types, zip codes, and source systems."""
    return queries.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    yr_min: int | None = Query(None, description="Minimum year"),
    yr_max: int | None = Query(None, description="Maximum year"),
    permit_type: str | None = Query(None, description="Filter by permit type"),
    zip_code: str | None = Query(None, description="Filter by zip code"),
):
    """Total permits, housing units, valuation, and median approval days."""
    return queries.get_overview(yr_min, yr_max, permit_type, zip_code)


@app.get("/permit-volume", response_model=list[PermitVolume])
def permit_volume(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
    permit_type: str | None = Query(None),
    zip_code: str | None = Query(None),
):
    """Monthly permit counts by type."""
    return queries.get_permit_volume(yr_min, yr_max, permit_type, zip_code)


@app.get("/housing-units", response_model=list[HousingUnits])
def housing_units(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
):
    """Annual dwelling units by income category (for RHNA tracking)."""
    return queries.get_housing_units(yr_min, yr_max)


@app.get("/approval-timelines", response_model=list[ApprovalTimeline])
def approval_timelines(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
    permit_type: str | None = Query(None),
    zip_code: str | None = Query(None),
):
    """Approval day statistics by type and zip code."""
    return queries.get_approval_timelines(yr_min, yr_max, permit_type, zip_code)


@app.get("/solar-permits", response_model=list[SolarPermit])
def solar_permits(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
    zip_code: str | None = Query(None),
):
    """Monthly solar permit counts with cumulative totals."""
    return queries.get_solar_permits(yr_min, yr_max, zip_code)


@app.get("/construction-by-zip", response_model=list[ConstructionByZip])
def construction_by_zip(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
    zip_code: str | None = Query(None),
):
    """Permits, valuation, and dwelling units by zip code and year."""
    return queries.get_construction_by_zip(yr_min, yr_max, zip_code)


@app.get("/top-permit-types", response_model=list[PermitTypeSummary])
def top_permit_types(
    yr_min: int | None = Query(None),
    yr_max: int | None = Query(None),
):
    """Summary stats per permit type."""
    return queries.get_top_permit_types(yr_min, yr_max)
