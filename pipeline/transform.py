"""Transform raw permit CSVs into a clean parquet + pre-aggregated tables."""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_RAW = _ROOT / "data" / "raw"
_PROCESSED = _ROOT / "data" / "processed"
_AGG = _ROOT / "data" / "aggregated"

# Paths for raw CSVs
_SET1_ACTIVE = str(_RAW / "set1_active.csv")
_SET1_CLOSED = str(_RAW / "set1_closed.csv")
_SET2_ACTIVE = str(_RAW / "set2_active.csv")
_SET2_CLOSED = str(_RAW / "set2_closed.csv")

_PERMITS_PARQUET = str(_PROCESSED / "permits.parquet")


def transform() -> None:
    """Run the full transform pipeline."""
    _PROCESSED.mkdir(parents=True, exist_ok=True)
    _AGG.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    # ── Stage 1: Load & normalize Set 1 (legacy system, 39 cols) ──
    print("  Loading Set 1 (legacy) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE set1_raw AS
        SELECT * FROM read_csv(
            ['{_SET1_ACTIVE}', '{_SET1_CLOSED}'],
            union_by_name = true,
            auto_detect = true,
            ignore_errors = true
        )
    """)
    row_count_1 = con.execute("SELECT COUNT(*) FROM set1_raw").fetchone()[0]
    print(f"    Set 1 rows: {row_count_1:,}")

    con.execute("""
        CREATE OR REPLACE TABLE set1 AS
        SELECT
            CAST(APPROVAL_ID AS VARCHAR)        AS approval_id,
            CAST(PROJECT_ID AS VARCHAR)         AS project_id,
            CAST(DEVELOPMENT_ID AS VARCHAR)     AS development_id,
            TRIM(PROJECT_TYPE)                  AS project_type,
            TRIM(PROJECT_STATUS)                AS project_status,
            TRIM(PROJECT_PROCESSING_CODE)       AS project_processing_code,
            TRIM(PROJECT_TITLE)                 AS project_title,
            TRIM(PROJECT_SCOPE)                 AS project_scope,
            TRY_CAST(DATE_PROJECT_CREATE AS DATE)   AS date_project_create,
            TRY_CAST(DATE_PROJECT_COMPLETE AS DATE) AS date_project_complete,
            CAST(JOB_ID AS VARCHAR)             AS job_id,
            TRIM(ADDRESS_JOB)                   AS address,
            TRIM(CAST(JOB_APN AS VARCHAR))      AS apn,
            TRIM(CAST(JOB_BC_CODE AS VARCHAR))  AS bc_code,
            TRIM(JOB_BC_CODE_DESCRIPTION)       AS bc_code_description,
            TRY_CAST(LAT_JOB AS DOUBLE)         AS lat,
            TRY_CAST(LNG_JOB AS DOUBLE)         AS lng,
            TRIM(APPROVAL_TYPE)                 AS approval_type,
            TRIM(APPROVAL_STATUS)               AS approval_status,
            TRIM(APPROVAL_SCOPE)                AS approval_scope,
            TRY_CAST(DATE_APPROVAL_CREATE AS DATE)  AS date_approval_create,
            TRY_CAST(DATE_APPROVAL_ISSUE AS DATE)   AS date_approval_issue,
            TRY_CAST(DATE_APPROVAL_EXPIRE AS DATE)  AS date_approval_expire,
            TRY_CAST(DATE_APPROVAL_CLOSE AS DATE)   AS date_approval_close,
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
            -- Set 1 has no ADU/JADU columns — NULL-fill
            NULL::INTEGER AS adu_extremely_low,
            NULL::INTEGER AS adu_very_low,
            NULL::INTEGER AS adu_low,
            NULL::INTEGER AS adu_moderate,
            NULL::INTEGER AS adu_above_moderate,
            NULL::INTEGER AS adu_bonus,
            NULL::INTEGER AS adu_total,
            NULL::INTEGER AS jadu_extremely_low,
            NULL::INTEGER AS jadu_very_low,
            NULL::INTEGER AS jadu_low,
            NULL::INTEGER AS jadu_moderate,
            NULL::INTEGER AS jadu_above_moderate,
            NULL::INTEGER AS jadu_bonus,
            NULL::INTEGER AS jadu_total,
            TRIM(APPROVAL_PERMIT_HOLDER)        AS permit_holder,
            'legacy'                            AS source_system
        FROM set1_raw
    """)

    # ── Stage 2: Load & normalize Set 2 (current system, 46+ cols) ──
    print("  Loading Set 2 (current) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE set2_raw AS
        SELECT * FROM read_csv(
            ['{_SET2_ACTIVE}', '{_SET2_CLOSED}'],
            union_by_name = true,
            auto_detect = true,
            ignore_errors = true
        )
    """)
    row_count_2 = con.execute("SELECT COUNT(*) FROM set2_raw").fetchone()[0]
    print(f"    Set 2 rows: {row_count_2:,}")

    con.execute("""
        CREATE OR REPLACE TABLE set2 AS
        SELECT
            CAST(APPROVAL_ID AS VARCHAR)        AS approval_id,
            CAST(PROJECT_ID AS VARCHAR)         AS project_id,
            NULL::VARCHAR                       AS development_id,
            NULL::VARCHAR                       AS project_type,
            NULL::VARCHAR                       AS project_status,
            TRIM(PROJECT_PROCESSING_CODE)       AS project_processing_code,
            TRIM(PROJECT_TITLE)                 AS project_title,
            TRIM(PROJECT_SCOPE)                 AS project_scope,
            TRY_CAST(DATE_PROJECT_CREATE AS DATE)   AS date_project_create,
            TRY_CAST(DATE_PROJECT_COMPLETE AS DATE) AS date_project_complete,
            CAST(JOB_ID AS VARCHAR)             AS job_id,
            TRIM(ADDRESS_JOB)                   AS address,
            TRIM(CAST(JOB_APN AS VARCHAR))      AS apn,
            TRIM(CAST(JOB_BC_CODE AS VARCHAR))  AS bc_code,
            TRIM(JOB_BC_CODE_DESCRIPTION)       AS bc_code_description,
            TRY_CAST(LAT_JOB AS DOUBLE)         AS lat,
            TRY_CAST(LNG_JOB AS DOUBLE)         AS lng,
            TRIM(APPROVAL_TYPE)                 AS approval_type,
            TRIM(APPROVAL_STATUS)               AS approval_status,
            TRIM(APPROVAL_SCOPE)                AS approval_scope,
            TRY_CAST(DATE_APPROVAL_CREATE AS DATE)  AS date_approval_create,
            TRY_CAST(DATE_APPROVAL_ISSUE AS DATE)   AS date_approval_issue,
            TRY_CAST(DATE_APPROVAL_EXPIRE AS DATE)  AS date_approval_expire,
            TRY_CAST(DATE_APPROVAL_CLOSE AS DATE)   AS date_approval_close,
            TRY_CAST(APPROVAL_VALUATION AS DOUBLE)  AS valuation,
            NULL::INTEGER                       AS du_net_change,
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
            'current'                           AS source_system
        FROM set2_raw
    """)

    # ── Stage 3: Union into single permits table with derived fields ──
    print("  Unioning sets + deriving fields ...")
    con.execute("""
        CREATE OR REPLACE TABLE permits_union AS
        SELECT * FROM set1
        UNION ALL
        SELECT * FROM set2
    """)

    total_raw = con.execute("SELECT COUNT(*) FROM permits_union").fetchone()[0]
    print(f"    Union total: {total_raw:,}")

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
                SUM(permit_count) OVER (ORDER BY year, month) AS cumulative_total
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
