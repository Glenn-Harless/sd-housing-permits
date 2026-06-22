"""Transform raw permit CSVs into a clean parquet + pre-aggregated tables."""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_RAW = _ROOT / "data" / "raw"
_PROCESSED = _ROOT / "data" / "processed"
_AGG = _ROOT / "data" / "aggregated"

# Paths for raw CSVs. Upstream consolidated the legacy/current systems into one
# dataset split by status, so there are now just two permit files: active + closed.
_ACTIVE = str(_RAW / "active.csv")
_CLOSED = str(_RAW / "closed.csv")

_PERMITS_PARQUET = str(_PROCESSED / "permits.parquet")


def transform() -> None:
    """Run the full transform pipeline."""
    _PROCESSED.mkdir(parents=True, exist_ok=True)
    _AGG.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ── Stage 1: Load active + closed (one consolidated dataset, 54 cols) ──
    # The active and closed files are disjoint (a permit is in exactly one), so
    # reading them together is a simple concatenation with no double-counting.
    print("  Loading permits (active + closed) ...")
    # Read every column as VARCHAR: PROJECT_ID and other ID columns mix numeric
    # legacy values with prefixed values like "PRJ-1155742", so type auto-detection
    # would infer BIGINT from the sample and then (with ignore_errors) silently drop
    # every row that doesn't match. The normalize step below casts each field
    # explicitly, so reading as text loses nothing and keeps the row count exact.
    con.execute(f"""
        CREATE OR REPLACE TABLE permits_raw AS
        SELECT * FROM read_csv(
            ['{_ACTIVE}', '{_CLOSED}'],
            union_by_name = true,
            auto_detect = true,
            all_varchar = true,
            ignore_errors = true
        )
    """)
    row_count = con.execute("SELECT COUNT(*) FROM permits_raw").fetchone()[0]
    print(f"    Raw rows: {row_count:,}")

    # ── Stage 2: Normalize to the canonical schema ──
    # The consolidated dataset carries DU + ADU + JADU income-level columns for
    # every row, so the income/ADU fields are wired to the real columns.
    print("  Normalizing ...")
    con.execute("""
        CREATE OR REPLACE TABLE permits_union AS
        SELECT
            CAST(APPROVAL_ID AS VARCHAR)        AS approval_id,
            CAST(PROJECT_ID AS VARCHAR)         AS project_id,
            CAST(DEVELOPMENT_ID AS VARCHAR)     AS development_id,
            TRIM(PROJECT_TYPE)                  AS project_type,
            TRIM(PROJECT_STATUS)                AS project_status,
            TRIM(PROJECT_PROCESSING_CODE)       AS project_processing_code,
            TRIM(PROJECT_TITLE)                 AS project_title,
            TRIM(PROJECT_SCOPE)                 AS project_scope,
            TRY_CAST(PROJECT_CREATE_DATE AS DATE)         AS date_project_create,
            TRY_CAST(PROJECT_DEEMEDCOMPLETE_DATE AS DATE) AS date_project_complete,
            CAST(JOB_ID AS VARCHAR)             AS job_id,
            TRIM(GIS_ADDRESS)                   AS address,
            TRIM(CAST(GIS_APN AS VARCHAR))      AS apn,
            TRIM(CAST(JOB_BC_CODE AS VARCHAR))  AS bc_code,
            TRIM(JOB_BC_CODE_DESCRIPTION)       AS bc_code_description,
            TRY_CAST(GIS_LATITUDE AS DOUBLE)    AS lat,
            TRY_CAST(GIS_LONGITUDE AS DOUBLE)   AS lng,
            TRIM(APPROVAL_TYPE)                 AS approval_type,
            TRIM(APPROVAL_STATUS)               AS approval_status,
            TRIM(APPROVAL_SCOPE)                AS approval_scope,
            TRY_CAST(APPROVAL_CREATE_DATE AS DATE)  AS date_approval_create,
            TRY_CAST(APPROVAL_ISSUE_DATE AS DATE)   AS date_approval_issue,
            TRY_CAST(APPROVAL_EXPIRE_DATE AS DATE)  AS date_approval_expire,
            TRY_CAST(APPROVAL_CLOSE_DATE AS DATE)   AS date_approval_close,
            TRY_CAST(APPROVAL_VALUATION AS DOUBLE)  AS valuation,
            TRY_CAST(APPROVAL_DU_NET_CHANGE AS INTEGER) AS du_net_change,
            TRY_CAST(APPROVAL_STORIES AS INTEGER)       AS stories,
            TRY_CAST(APPROVAL_FLOOR_AREA AS DOUBLE)     AS floor_area,
            TRY_CAST(APPROVAL_DU_EXTREMELY_LOW AS INTEGER)  AS du_extremely_low,
            TRY_CAST(APPROVAL_DU_VERY_LOW AS INTEGER)       AS du_very_low,
            TRY_CAST(APPROVAL_DU_LOW AS INTEGER)            AS du_low,
            TRY_CAST(APPROVAL_DU_MODERATE AS INTEGER)       AS du_moderate,
            TRY_CAST(APPROVAL_DU_ABOVE_MODERATE AS INTEGER) AS du_above_moderate,
            TRY_CAST(APPROVAL_DU_FUTURE_DEMO AS INTEGER)    AS du_future_demo,
            TRY_CAST(APPROVAL_DU_BONUS AS INTEGER)          AS du_bonus,
            TRY_CAST(APPROVAL_ADU_EXTREMELY_LOW AS INTEGER) AS adu_extremely_low,
            TRY_CAST(APPROVAL_ADU_VERY_LOW AS INTEGER)      AS adu_very_low,
            TRY_CAST(APPROVAL_ADU_LOW AS INTEGER)           AS adu_low,
            TRY_CAST(APPROVAL_ADU_MODERATE AS INTEGER)      AS adu_moderate,
            TRY_CAST(APPROVAL_ADU_ABOVE_MODERATE AS INTEGER) AS adu_above_moderate,
            TRY_CAST(APPROVAL_ADU_BONUS AS INTEGER)         AS adu_bonus,
            TRY_CAST(APPROVAL_ADU_TOTAL AS INTEGER)         AS adu_total,
            TRY_CAST(APPROVAL_JADU_EXTREMELY_LOW AS INTEGER) AS jadu_extremely_low,
            TRY_CAST(APPROVAL_JADU_VERY_LOW AS INTEGER)      AS jadu_very_low,
            TRY_CAST(APPROVAL_JADU_LOW AS INTEGER)           AS jadu_low,
            TRY_CAST(APPROVAL_JADU_MODERATE AS INTEGER)      AS jadu_moderate,
            TRY_CAST(APPROVAL_JADU_ABOVE_MODERATE AS INTEGER) AS jadu_above_moderate,
            TRY_CAST(APPROVAL_JADU_BONUS AS INTEGER)         AS jadu_bonus,
            TRY_CAST(APPROVAL_JADU_TOTAL AS INTEGER)         AS jadu_total,
            TRIM(APPROVAL_PERMIT_HOLDER)        AS permit_holder,
            -- Upstream merged the former legacy/current systems into one dataset;
            -- the source_system distinction no longer exists, so it is a constant.
            'consolidated'                      AS source_system
        FROM permits_raw
    """)

    total_raw = con.execute("SELECT COUNT(*) FROM permits_union").fetchone()[0]
    print(f"    Normalized total: {total_raw:,}")

    # Derived fields + dedup + geo filter
    con.execute("""
        CREATE OR REPLACE TABLE permits AS
        WITH derived AS (
            SELECT
                *,
                -- zip code from address (only keep valid SD zips: 920xx-921xx)
                CASE
                    WHEN REGEXP_EXTRACT(address, '(9[12][0-9]{3})', 1) != ''
                    THEN REGEXP_EXTRACT(address, '(9[12][0-9]{3})', 1)
                    ELSE NULL
                END AS zip_code,

                -- approval timeline
                CASE
                    WHEN date_approval_issue IS NOT NULL
                         AND date_approval_create IS NOT NULL
                         AND DATEDIFF('day', date_approval_create, date_approval_issue) >= 0
                    THEN DATEDIFF('day', date_approval_create, date_approval_issue)
                    ELSE NULL
                END AS approval_days,

                -- year/month from issue date (fallback to create date)
                YEAR(COALESCE(date_approval_issue, date_approval_create))  AS approval_year,
                MONTH(COALESCE(date_approval_issue, date_approval_create)) AS approval_month,

                -- approval type clean (normalized grouping)
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%PHOTOVOLTAIC%'
                      OR UPPER(TRIM(approval_type)) LIKE '%PV%'
                      OR UPPER(TRIM(approval_type)) LIKE '%SOLAR%'
                    THEN 'Solar/PV'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%COMBINATION BUILDING%'
                      OR UPPER(TRIM(approval_type)) = 'BUILDING PERMIT'
                      OR UPPER(TRIM(approval_type)) LIKE 'BUILDING PERMIT%'
                    THEN 'Building Permit'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%ELECTRICAL%'
                    THEN 'Electrical'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%PLUMBING%'
                    THEN 'Plumbing'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%MECHANICAL%'
                    THEN 'Mechanical'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%FIRE%'
                    THEN 'Fire'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%RIGHT OF WAY%'
                      OR UPPER(TRIM(approval_type)) LIKE '%ROW%'
                    THEN 'Right of Way'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%SIGN%'
                    THEN 'Sign'
                    ELSE 'Other'
                END AS approval_type_clean,

                -- is_housing: bc_code starts with '10' (new residential) OR building permit with DU > 0
                CASE
                    WHEN bc_code IS NOT NULL AND bc_code LIKE '10%' THEN TRUE
                    WHEN (UPPER(TRIM(approval_type)) LIKE '%BUILDING PERMIT%'
                          OR UPPER(TRIM(approval_type)) LIKE '%COMBINATION BUILDING%')
                         AND (COALESCE(du_extremely_low, 0) + COALESCE(du_very_low, 0)
                              + COALESCE(du_low, 0) + COALESCE(du_moderate, 0)
                              + COALESCE(du_above_moderate, 0)
                              + COALESCE(adu_total, 0) + COALESCE(jadu_total, 0)) > 0
                    THEN TRUE
                    ELSE FALSE
                END AS is_housing,

                -- is_solar
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%PHOTOVOLTAIC%'
                      OR UPPER(TRIM(approval_type)) LIKE '%PV%'
                      OR UPPER(TRIM(approval_type)) LIKE '%SOLAR%'
                    THEN TRUE
                    ELSE FALSE
                END AS is_solar,

                -- is_adu
                CASE
                    WHEN bc_code = '4333' THEN TRUE
                    WHEN COALESCE(adu_total, 0) > 0 THEN TRUE
                    WHEN COALESCE(jadu_total, 0) > 0 THEN TRUE
                    ELSE FALSE
                END AS is_adu,

                -- total dwelling units (all DU + ADU + JADU)
                COALESCE(du_extremely_low, 0) + COALESCE(du_very_low, 0)
                + COALESCE(du_low, 0) + COALESCE(du_moderate, 0)
                + COALESCE(du_above_moderate, 0)
                + COALESCE(du_future_demo, 0) + COALESCE(du_bonus, 0)
                + COALESCE(adu_total, 0) + COALESCE(jadu_total, 0)
                AS total_du
            FROM permits_union
        ),
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY approval_id
                    ORDER BY date_approval_close DESC NULLS LAST
                ) AS _rn
            FROM derived
        )
        SELECT * EXCLUDE (_rn)
        FROM deduped
        WHERE _rn = 1
          -- geo filter: San Diego bounds
          AND (lat IS NULL OR (lat BETWEEN 32.5 AND 33.3))
          AND (lng IS NULL OR (lng BETWEEN -117.7 AND -116.8))
    """)

    final_count = con.execute("SELECT COUNT(*) FROM permits").fetchone()[0]
    print(f"    Final permits (deduped + geo-filtered): {final_count:,}")

    # ── Export main parquet ──
    print(f"  Exporting {_PERMITS_PARQUET} ...")
    con.execute(f"""
        COPY permits TO '{_PERMITS_PARQUET}'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    size_mb = Path(_PERMITS_PARQUET).stat().st_size / (1024 * 1024)
    print(f"    permits.parquet: {size_mb:.1f} MB")

    # ── Build aggregations ──
    _build_aggregations(con)

    con.close()
    print("  Transform complete.")


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build 8 pre-aggregated parquet files for dashboard/API."""

    # 1. permit_volume_monthly — monthly counts by approval_type_clean, source_system
    print("  Aggregating: permit_volume_monthly ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                approval_month AS month,
                approval_type_clean,
                source_system,
                COUNT(*) AS permit_count
            FROM permits
            WHERE approval_year IS NOT NULL
            GROUP BY approval_year, approval_month, approval_type_clean, source_system
            ORDER BY year, month
        ) TO '{_AGG}/permit_volume_monthly.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 2. housing_units_by_year — annual DU counts by income category
    print("  Aggregating: housing_units_by_year ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                SUM(COALESCE(du_extremely_low, 0)) AS du_extremely_low,
                SUM(COALESCE(du_very_low, 0))      AS du_very_low,
                SUM(COALESCE(du_low, 0))            AS du_low,
                SUM(COALESCE(du_moderate, 0))       AS du_moderate,
                SUM(COALESCE(du_above_moderate, 0)) AS du_above_moderate,
                SUM(COALESCE(adu_total, 0))         AS adu_total,
                SUM(COALESCE(jadu_total, 0))        AS jadu_total,
                SUM(total_du)                       AS total_du
            FROM permits
            WHERE approval_year IS NOT NULL AND is_housing = TRUE
            GROUP BY approval_year
            ORDER BY year
        ) TO '{_AGG}/housing_units_by_year.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 3. approval_timelines — median/avg/p90 approval_days by type, zip, year
    print("  Aggregating: approval_timelines ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                approval_type_clean,
                zip_code,
                COUNT(*) AS permit_count,
                MEDIAN(approval_days) AS median_days,
                AVG(approval_days)::INTEGER AS avg_days,
                QUANTILE_CONT(approval_days, 0.9)::INTEGER AS p90_days
            FROM permits
            WHERE approval_days IS NOT NULL AND approval_year IS NOT NULL
            GROUP BY approval_year, approval_type_clean, zip_code
            ORDER BY year, approval_type_clean
        ) TO '{_AGG}/approval_timelines.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 4. solar_permits_monthly — monthly PV counts + cumulative
    print("  Aggregating: solar_permits_monthly ...")
    con.execute(f"""
        COPY (
            SELECT
                year, month, zip_code, permit_count,
                SUM(permit_count) OVER (PARTITION BY zip_code ORDER BY year, month) AS cumulative_total
            FROM (
                SELECT
                    approval_year AS year,
                    approval_month AS month,
                    zip_code,
                    COUNT(*) AS permit_count
                FROM permits
                WHERE is_solar = TRUE AND approval_year IS NOT NULL
                GROUP BY approval_year, approval_month, zip_code
            )
            ORDER BY year, month
        ) TO '{_AGG}/solar_permits_monthly.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 5. map_points — full geo dataset for mapping
    print("  Aggregating: map_points ...")
    con.execute(f"""
        COPY (
            SELECT
                lat, lng,
                approval_type_clean,
                approval_year,
                valuation,
                total_du,
                is_housing,
                is_solar,
                zip_code
            FROM permits
            WHERE lat IS NOT NULL AND lng IS NOT NULL
        ) TO '{_AGG}/map_points.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 6. top_permit_types — summary stats per approval_type
    print("  Aggregating: top_permit_types ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_type_clean,
                COUNT(*) AS permit_count,
                AVG(valuation)::BIGINT AS avg_valuation,
                MEDIAN(approval_days) AS median_approval_days
            FROM permits
            GROUP BY approval_type_clean
            ORDER BY permit_count DESC
        ) TO '{_AGG}/top_permit_types.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 7. construction_by_zip — permits, valuation, DUs by zip and year
    print("  Aggregating: construction_by_zip ...")
    con.execute(f"""
        COPY (
            SELECT
                zip_code,
                approval_year AS year,
                COUNT(*) AS permit_count,
                SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation,
                SUM(total_du) AS total_du
            FROM permits
            WHERE zip_code IS NOT NULL AND approval_year IS NOT NULL
            GROUP BY zip_code, approval_year
            ORDER BY zip_code, year
        ) TO '{_AGG}/construction_by_zip.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 8. bc_code_summary — building type breakdown (with year + source for filtering)
    print("  Aggregating: bc_code_summary ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                source_system,
                bc_code,
                bc_code_description,
                COUNT(*) AS permit_count,
                SUM(total_du) AS total_du,
                SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation
            FROM permits
            WHERE bc_code IS NOT NULL AND approval_year IS NOT NULL
            GROUP BY approval_year, source_system, bc_code, bc_code_description
            ORDER BY permit_count DESC
        ) TO '{_AGG}/bc_code_summary.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 9. permit_summary — overview-level stats by year/type/zip/source
    #    Replaces direct queries against the full permits.parquet
    print("  Aggregating: permit_summary ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                approval_type_clean,
                zip_code,
                source_system,
                COUNT(*) AS permit_count,
                SUM(total_du) AS total_du,
                SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation,
                COUNT(approval_days) AS count_with_days,
                SUM(approval_days) AS sum_approval_days,
                MEDIAN(approval_days) AS median_approval_days
            FROM permits
            WHERE approval_year IS NOT NULL
            GROUP BY approval_year, approval_type_clean, zip_code, source_system
            ORDER BY year
        ) TO '{_AGG}/permit_summary.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    print("  All aggregations complete.")


if __name__ == "__main__":
    transform()
