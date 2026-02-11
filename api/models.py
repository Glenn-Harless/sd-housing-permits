"""Pydantic response models for FastAPI's auto-generated OpenAPI docs."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    years: list[int]
    permit_types: list[str]
    zip_codes: list[str]
    source_systems: list[str]


class OverviewResponse(BaseModel):
    total_permits: int
    total_du: int | None
    total_valuation: int | None
    median_approval_days: int | None


class PermitVolume(BaseModel):
    year: int
    month: int
    approval_type_clean: str
    permit_count: int


class HousingUnits(BaseModel):
    year: int
    du_extremely_low: int | None
    du_very_low: int | None
    du_low: int | None
    du_moderate: int | None
    du_above_moderate: int | None
    adu_total: int | None
    jadu_total: int | None
    total_du: int | None


class ApprovalTimeline(BaseModel):
    year: int
    approval_type_clean: str
    zip_code: str | None
    permit_count: int
    median_days: float | None
    avg_days: int | None
    p90_days: int | None


class SolarPermit(BaseModel):
    year: int
    month: int
    zip_code: str | None
    permit_count: int
    cumulative_total: int | None


class ConstructionByZip(BaseModel):
    zip_code: str
    year: int
    permit_count: int
    total_valuation: int | None
    total_du: int | None


class PermitTypeSummary(BaseModel):
    approval_type_clean: str
    permit_count: int
    avg_valuation: int | None
    median_approval_days: int | None
