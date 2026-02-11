"""San Diego Housing Permits Dashboard."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="SD Housing Permits",
    page_icon="🏗️",
    layout="wide",
)

_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")


# ── Helpers ──────────────────────────────────────────────────


def query(sql: str):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql).fetchdf()


def _fmt_number(n: float, prefix: str = "") -> str:
    """Format large numbers for display."""
    if abs(n) >= 1_000_000_000:
        return f"{prefix}{n / 1_000_000_000:.1f}B"
    if abs(n) >= 1_000_000:
        return f"{prefix}{n / 1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{prefix}{n / 1_000:.1f}K"
    return f"{prefix}{n:,.0f}"


# ── Sidebar ──────────────────────────────────────────────────

st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    years = sorted(
        query(f"SELECT DISTINCT year FROM '{_AGG}/permit_volume_monthly.parquet' WHERE year IS NOT NULL")
        ["year"].dropna().astype(int).tolist()
    )
    types = sorted(
        query(f"SELECT DISTINCT approval_type_clean FROM '{_AGG}/top_permit_types.parquet'")
        ["approval_type_clean"].tolist()
    )
    zips = sorted(
        query(f"SELECT DISTINCT CAST(zip_code AS VARCHAR) AS zip_code FROM '{_AGG}/construction_by_zip.parquet' WHERE zip_code IS NOT NULL ORDER BY zip_code")
        ["zip_code"].astype(str).tolist()
    )
    return years, types, zips


all_years, all_types, all_zips = _sidebar_options()

year_range = st.sidebar.slider(
    "Year Range",
    min_value=int(min(all_years)),
    max_value=int(max(all_years)),
    value=(2015, int(max(all_years))),
)

selected_types = st.sidebar.multiselect(
    "Permit Type",
    options=all_types,
    default=None,
    placeholder="All permit types",
)

selected_zips = st.sidebar.multiselect(
    "Zip Code",
    options=all_zips,
    default=None,
    placeholder="All zip codes",
)

source_system = st.sidebar.selectbox(
    "Source System",
    options=["All", "Legacy (2002-2022)", "Current (2018+)"],
    index=0,
)

_source_map = {
    "All": None,
    "Legacy (2002-2022)": "legacy",
    "Current (2018+)": "current",
}
_selected_source = _source_map[source_system]

with st.sidebar.expander("About source systems"):
    st.markdown("""
**Legacy (2002–2022)** — The city's original permitting system.
Has `DEVELOPMENT_ID` and net dwelling-unit change fields but
no ADU/JADU breakdowns.

**Current (2018+)** — The newer cloud-based system that replaced
it. Includes detailed ADU and JADU unit counts by income level.

The two systems overlap from 2018–2022. Records are deduped by
approval ID, keeping the most recent close date.
""")


def _where(
    *,
    year_col: str = "year",
    type_col: str = "approval_type_clean",
    zip_col: str = "zip_code",
    source_col: str | None = None,
    has_type: bool = True,
    has_zip: bool = True,
    has_source: bool = False,
) -> str:
    """Build WHERE clause from sidebar filter selections."""
    clauses = [f"{year_col} BETWEEN {year_range[0]} AND {year_range[1]}"]
    if selected_types and has_type:
        escaped = ", ".join(f"'{t}'" for t in selected_types)
        clauses.append(f"{type_col} IN ({escaped})")
    if selected_zips and has_zip:
        escaped = ", ".join(f"'{z}'" for z in selected_zips)
        clauses.append(f"{zip_col} IN ({escaped})")
    if _selected_source and has_source and source_col:
        clauses.append(f"{source_col} = '{_selected_source}'")
    return "WHERE " + " AND ".join(clauses)


# ── Title ────────────────────────────────────────────────────

st.title("San Diego Housing Permits")
st.caption("Development permit data from the City of San Diego (2002–present)")

# ── Tabs ─────────────────────────────────────────────────────

tab_overview, tab_housing, tab_map, tab_timelines, tab_solar = st.tabs(
    ["Overview", "Housing Progress", "Where SD Is Building", "Approval Timelines", "Solar Adoption"]
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tab 1: Overview
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with tab_overview:
    w = _where(
        source_col="source_system",
        has_source=True,
    )
    overview = query(f"""
        SELECT
            SUM(permit_count) AS total_permits,
            SUM(total_du) AS total_du,
            SUM(total_valuation)::BIGINT AS total_valuation,
            CAST(SUM(median_approval_days * count_with_days)
                 / NULLIF(SUM(count_with_days), 0) AS INTEGER) AS median_days
        FROM '{_AGG}/permit_summary.parquet'
        {w}
    """)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Permits", _fmt_number(overview["total_permits"].iloc[0]))
    c2.metric("Housing Units Permitted", _fmt_number(overview["total_du"].iloc[0]))
    c3.metric("Total Valuation", _fmt_number(overview["total_valuation"].iloc[0], "$"))
    md = overview["median_days"].iloc[0]
    c4.metric("Median Approval Days", f"{int(md)}" if md and md == md else "N/A")

    st.subheader("Permit Volume Over Time")
    w_vol = _where(has_zip=False, source_col="source_system", has_source=True)
    vol = query(f"""
        SELECT year, month, approval_type_clean,
               SUM(permit_count) AS permits
        FROM '{_AGG}/permit_volume_monthly.parquet'
        {w_vol}
        GROUP BY year, month, approval_type_clean
        ORDER BY year, month
    """)
    if not vol.empty:
        vol["date"] = vol.apply(lambda r: f"{int(r['year'])}-{int(r['month']):02d}-01", axis=1)
        fig_vol = px.line(
            vol,
            x="date",
            y="permits",
            color="approval_type_clean",
            labels={"date": "Month", "permits": "Permits", "approval_type_clean": "Type"},
        )
        fig_vol.update_layout(height=400, legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_vol, use_container_width=True)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top 10 Permit Types")
        w_top = _where(has_zip=False)
        top_types = query(f"""
            SELECT approval_type_clean, SUM(permit_count) AS total
            FROM '{_AGG}/permit_volume_monthly.parquet'
            {w_top}
            GROUP BY approval_type_clean
            ORDER BY total DESC
            LIMIT 10
        """)
        if not top_types.empty:
            fig_top = px.bar(
                top_types,
                x="total",
                y="approval_type_clean",
                orientation="h",
                labels={"total": "Permits", "approval_type_clean": "Type"},
            )
            fig_top.update_layout(height=350, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_top, use_container_width=True)

    with col_right:
        st.subheader("Permits by Building Code")
        w_bc = _where(
            has_type=False,
            has_zip=False,
            source_col="source_system",
            has_source=True,
        )
        bc = query(f"""
            SELECT bc_code_description AS description,
                   SUM(permit_count) AS total
            FROM '{_AGG}/bc_code_summary.parquet'
            {w_bc}
            GROUP BY bc_code_description
            ORDER BY total DESC
            LIMIT 10
        """)
        if not bc.empty:
            fig_bc = px.bar(
                bc,
                x="total",
                y="description",
                orientation="h",
                labels={"total": "Permits", "description": "Building Type"},
            )
            fig_bc.update_layout(height=350, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_bc, use_container_width=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tab 2: Housing Progress
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with tab_housing:
    RHNA_TARGET = 108_036  # 6th cycle RHNA allocation for City of San Diego

    with st.expander("What is RHNA? How do I read this tab?", expanded=False):
        st.markdown("""
**RHNA** (Regional Housing Needs Assessment) is a state-mandated process that
determines how many new housing units each California city must plan for.
San Diego's **6th cycle allocation (2021–2029) is 108,036 units** across five
income categories:

| Category | Who it serves | Example (2-person household) |
|---|---|---|
| **Extremely Low** | < 30% of Area Median Income (AMI) | < ~$32K/yr |
| **Very Low** | 30–50% AMI | ~$32–53K/yr |
| **Low** | 50–80% AMI | ~$53–85K/yr |
| **Moderate** | 80–120% AMI | ~$85–128K/yr |
| **Above Moderate** | > 120% AMI | > ~$128K/yr |

**How to read the charts below:**
- **Gauge** — Blue bar shows cumulative permitted dwelling units vs. the
  108,036 target. Red line = target. Getting close means the city is on track.
- **Income stacked area** — Shows the mix of income levels being built each
  year. Ideally all bands grow, not just above-moderate.
- **ADU/JADU** — Accessory Dwelling Units (granny flats, garage conversions)
  and Junior ADUs (converted bedrooms). These are a major policy lever for
  affordable infill housing.
- **Top zip codes** — Where the most housing units are being permitted.

*Note: "Permitted" means a building permit was issued — not that the unit is
built or occupied yet. Actual construction typically follows 1–3 years later.*
""")

    hu = query(f"""
        SELECT * FROM '{_AGG}/housing_units_by_year.parquet'
        WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY year
    """)

    # RHNA progress gauge
    cumulative_du = int(hu["total_du"].sum()) if not hu.empty else 0
    pct = cumulative_du / RHNA_TARGET * 100

    st.subheader("RHNA Progress")
    c1, c2, c3 = st.columns(3)
    c1.metric("Units Permitted", f"{cumulative_du:,}")
    c2.metric("RHNA Target", f"{RHNA_TARGET:,}")
    c3.metric("Progress", f"{pct:.1f}%")

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=cumulative_du,
        delta={"reference": RHNA_TARGET},
        title={"text": "Permitted DUs vs RHNA Target"},
        gauge={
            "axis": {"range": [0, RHNA_TARGET * 1.2]},
            "bar": {"color": "#1f77b4"},
            "steps": [
                {"range": [0, RHNA_TARGET], "color": "#e8e8e8"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": RHNA_TARGET,
            },
        },
    ))
    fig_gauge.update_layout(height=300)
    st.plotly_chart(fig_gauge, use_container_width=True)

    # DU by income category — stacked area
    st.subheader("Housing Units by Income Category")
    if not hu.empty:
        income_cols = [
            "du_extremely_low", "du_very_low", "du_low",
            "du_moderate", "du_above_moderate",
        ]
        fig_income = go.Figure()
        colors = ["#d62728", "#ff7f0e", "#ffbb78", "#2ca02c", "#1f77b4"]
        for col, color in zip(income_cols, colors):
            label = col.replace("du_", "").replace("_", " ").title()
            fig_income.add_trace(go.Scatter(
                x=hu["year"],
                y=hu[col],
                mode="lines",
                stackgroup="one",
                name=label,
                line=dict(color=color),
            ))
        fig_income.update_layout(
            height=400,
            yaxis_title="Dwelling Units",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig_income, use_container_width=True)

    # ADU/JADU trend
    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("ADU / JADU Trend")
        if not hu.empty:
            fig_adu = go.Figure()
            fig_adu.add_trace(go.Bar(x=hu["year"], y=hu["adu_total"], name="ADU"))
            fig_adu.add_trace(go.Bar(x=hu["year"], y=hu["jadu_total"], name="JADU"))
            fig_adu.update_layout(
                barmode="stack",
                height=350,
                yaxis_title="Units",
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig_adu, use_container_width=True)

    with col_right:
        st.subheader("Total Housing Units by Year")
        if not hu.empty:
            fig_hu_yr = px.bar(
                hu, x="year", y="total_du",
                labels={"year": "Year", "total_du": "Dwelling Units"},
            )
            fig_hu_yr.update_layout(height=350)
            st.plotly_chart(fig_hu_yr, use_container_width=True)

    # Top zip codes by housing production
    st.subheader("Top Zip Codes by Housing Production")
    w_zip = _where(has_type=False)
    top_zips = query(f"""
        SELECT CAST(zip_code AS VARCHAR) AS zip_code, SUM(total_du) AS total_du
        FROM '{_AGG}/construction_by_zip.parquet'
        {w_zip}
        AND zip_code IS NOT NULL
        GROUP BY zip_code
        HAVING SUM(total_du) > 0
        ORDER BY total_du DESC
        LIMIT 15
    """)
    if not top_zips.empty:
        st.dataframe(top_zips, use_container_width=True, hide_index=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tab 3: Where SD Is Building
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with tab_map:
    import pydeck as pdk

    st.subheader("Permit Locations")
    st.caption(
        "Showing up to 200K sampled points. The map is most useful when you "
        "filter by **Permit Type** in the sidebar (e.g. just \"Building Permit\" "
        "or \"Solar/PV\") to reduce visual clutter."
    )

    color_by = st.radio(
        "Color by",
        ["Permit Type", "Valuation"],
        horizontal=True,
        key="map_color",
    )

    w_map = _where(
        year_col="approval_year",
        zip_col="zip_code",
    )
    map_df = query(f"""
        SELECT lat, lng, approval_type_clean, approval_year, valuation, total_du,
               is_housing, is_solar, zip_code
        FROM '{_AGG}/map_points.parquet'
        {w_map}
        USING SAMPLE 200000
    """)

    if not map_df.empty:
        type_colors = {
            "Building Permit": [31, 119, 180],
            "Solar/PV": [255, 127, 14],
            "Electrical": [44, 160, 44],
            "Plumbing": [214, 39, 40],
            "Mechanical": [148, 103, 189],
            "Fire": [140, 86, 75],
            "Right of Way": [227, 119, 194],
            "Sign": [127, 127, 127],
            "Other": [188, 189, 34],
        }

        if color_by == "Permit Type":
            map_df["color_r"] = map_df["approval_type_clean"].map(lambda t: type_colors.get(t, [188, 189, 34])[0])
            map_df["color_g"] = map_df["approval_type_clean"].map(lambda t: type_colors.get(t, [188, 189, 34])[1])
            map_df["color_b"] = map_df["approval_type_clean"].map(lambda t: type_colors.get(t, [188, 189, 34])[2])
        else:
            map_df["valuation"] = map_df["valuation"].fillna(0)
            max_val = max(map_df["valuation"].quantile(0.95), 1)
            norm = (map_df["valuation"] / max_val).clip(0, 1)
            map_df["color_r"] = (norm * 255).astype(int)
            map_df["color_g"] = ((1 - norm) * 200).astype(int)
            map_df["color_b"] = 50

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["lng", "lat"],
            get_color=["color_r", "color_g", "color_b", 160],
            get_radius=50,
            pickable=True,
        )
        view = pdk.ViewState(latitude=32.75, longitude=-117.15, zoom=10, pitch=0)
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view,
            tooltip={"text": "{approval_type_clean}\nYear: {approval_year}\nVal: ${valuation}\nDU: {total_du}"},
        )
        st.pydeck_chart(deck)

        # Legend for permit type colors
        if color_by == "Permit Type":
            legend_html = " &nbsp; ".join(
                f'<span style="color:rgb({c[0]},{c[1]},{c[2]})">●</span> {t}'
                for t, c in type_colors.items()
            )
            st.markdown(legend_html, unsafe_allow_html=True)
    else:
        st.info("No map data for the selected filters.")

    # Construction by zip code
    st.subheader("Construction Activity by Zip Code")
    w_cbz = _where(has_type=False)
    cbz = query(f"""
        SELECT CAST(zip_code AS VARCHAR) AS zip_code,
               SUM(permit_count) AS permits,
               SUM(total_valuation) AS valuation,
               SUM(total_du) AS du
        FROM '{_AGG}/construction_by_zip.parquet'
        {w_cbz}
        AND zip_code IS NOT NULL
        GROUP BY zip_code
        ORDER BY permits DESC
        LIMIT 20
    """)
    if not cbz.empty:
        fig_cbz = px.bar(
            cbz,
            x="zip_code",
            y="permits",
            hover_data=["valuation", "du"],
            labels={"zip_code": "Zip Code", "permits": "Permits"},
        )
        fig_cbz.update_layout(height=400, xaxis_type="category")
        st.plotly_chart(fig_cbz, use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tab 4: Approval Timelines
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with tab_timelines:
    st.subheader("Median Approval Days by Permit Type")
    w_tl = _where(has_zip=False)
    by_type = query(f"""
        SELECT approval_type_clean,
               SUM(permit_count) AS total_permits,
               CAST(SUM(median_days * permit_count) / NULLIF(SUM(permit_count), 0) AS INTEGER) AS weighted_median_days
        FROM '{_AGG}/approval_timelines.parquet'
        {w_tl}
        GROUP BY approval_type_clean
        ORDER BY weighted_median_days DESC
    """)
    if not by_type.empty:
        fig_tl_type = px.bar(
            by_type,
            x="weighted_median_days",
            y="approval_type_clean",
            orientation="h",
            labels={"weighted_median_days": "Median Days", "approval_type_clean": "Type"},
            text="weighted_median_days",
        )
        fig_tl_type.update_layout(height=350, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_tl_type, use_container_width=True)

    # Timeline trend over time
    st.subheader("Approval Timeline Trend")
    w_trend = _where(has_zip=False)
    trend = query(f"""
        SELECT year,
               CAST(SUM(median_days * permit_count) / NULLIF(SUM(permit_count), 0) AS INTEGER) AS weighted_median_days
        FROM '{_AGG}/approval_timelines.parquet'
        {w_trend}
        GROUP BY year
        ORDER BY year
    """)
    if not trend.empty:
        fig_trend = px.line(
            trend,
            x="year",
            y="weighted_median_days",
            labels={"year": "Year", "weighted_median_days": "Median Approval Days"},
            markers=True,
        )
        fig_trend.update_layout(height=350)
        st.plotly_chart(fig_trend, use_container_width=True)

    # Slowest vs fastest zip codes
    st.subheader("Slowest vs Fastest Zip Codes")
    w_zip_tl = _where(has_type=False)
    by_zip = query(f"""
        SELECT CAST(zip_code AS VARCHAR) AS zip_code,
               SUM(permit_count) AS total_permits,
               CAST(SUM(median_days * permit_count) / NULLIF(SUM(permit_count), 0) AS INTEGER) AS weighted_median_days
        FROM '{_AGG}/approval_timelines.parquet'
        {w_zip_tl}
        AND zip_code IS NOT NULL
        GROUP BY zip_code
        HAVING SUM(permit_count) >= 100
        ORDER BY weighted_median_days DESC
    """)

    if not by_zip.empty:
        col_slow, col_fast = st.columns(2)
        with col_slow:
            st.markdown("**Slowest (Top 10)**")
            slowest = by_zip.head(10)
            fig_slow = px.bar(
                slowest,
                x="weighted_median_days",
                y="zip_code",
                orientation="h",
                labels={"weighted_median_days": "Median Days", "zip_code": "Zip"},
                text="weighted_median_days",
            )
            fig_slow.update_layout(height=350, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_slow, use_container_width=True)

        with col_fast:
            st.markdown("**Fastest (Top 10)**")
            fastest = by_zip.tail(10).sort_values("weighted_median_days")
            fig_fast = px.bar(
                fastest,
                x="weighted_median_days",
                y="zip_code",
                orientation="h",
                labels={"weighted_median_days": "Median Days", "zip_code": "Zip"},
                text="weighted_median_days",
            )
            fig_fast.update_layout(height=350, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_fast, use_container_width=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Tab 5: Solar Adoption
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with tab_solar:
    # Cumulative solar permits
    st.subheader("Cumulative Solar Permits")
    solar_monthly = query(f"""
        SELECT year, month,
               SUM(permit_count) AS permits
        FROM '{_AGG}/solar_permits_monthly.parquet'
        WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
        {'AND zip_code IN (' + ','.join(f"'{z}'" for z in selected_zips) + ')' if selected_zips else ''}
        GROUP BY year, month
        ORDER BY year, month
    """)
    if not solar_monthly.empty:
        solar_monthly["date"] = solar_monthly.apply(
            lambda r: f"{int(r['year'])}-{int(r['month']):02d}-01", axis=1
        )
        solar_monthly["cumulative"] = solar_monthly["permits"].cumsum()
        fig_cum = px.line(
            solar_monthly,
            x="date",
            y="cumulative",
            labels={"date": "Month", "cumulative": "Cumulative Solar Permits"},
        )
        fig_cum.update_layout(height=350)
        st.plotly_chart(fig_cum, use_container_width=True)

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Solar Permits by Zip Code")
        solar_zip = query(f"""
            SELECT CAST(zip_code AS VARCHAR) AS zip_code, SUM(permit_count) AS permits
            FROM '{_AGG}/solar_permits_monthly.parquet'
            WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
            AND zip_code IS NOT NULL
            GROUP BY zip_code
            ORDER BY permits DESC
            LIMIT 15
        """)
        if not solar_zip.empty:
            fig_sz = px.bar(
                solar_zip,
                x="zip_code",
                y="permits",
                labels={"zip_code": "Zip Code", "permits": "Solar Permits"},
            )
            fig_sz.update_layout(height=350, xaxis_type="category")
            st.plotly_chart(fig_sz, use_container_width=True)

    with col_right:
        st.subheader("Monthly Solar Permit Trend")
        if not solar_monthly.empty:
            fig_sm = px.bar(
                solar_monthly,
                x="date",
                y="permits",
                labels={"date": "Month", "permits": "Solar Permits"},
            )
            fig_sm.update_layout(height=350)
            st.plotly_chart(fig_sm, use_container_width=True)

    # Solar as % of all permits
    st.subheader("Solar as % of All Permits")
    solar_pct = query(f"""
        WITH all_permits AS (
            SELECT year, SUM(permit_count) AS total
            FROM '{_AGG}/permit_volume_monthly.parquet'
            WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
            GROUP BY year
        ),
        solar AS (
            SELECT year, SUM(permit_count) AS solar_total
            FROM '{_AGG}/solar_permits_monthly.parquet'
            WHERE year BETWEEN {year_range[0]} AND {year_range[1]}
            GROUP BY year
        )
        SELECT a.year,
               COALESCE(s.solar_total, 0) AS solar_total,
               a.total,
               ROUND(COALESCE(s.solar_total, 0) * 100.0 / NULLIF(a.total, 0), 1) AS solar_pct
        FROM all_permits a
        LEFT JOIN solar s ON a.year = s.year
        ORDER BY a.year
    """)
    if not solar_pct.empty:
        fig_pct = px.line(
            solar_pct,
            x="year",
            y="solar_pct",
            labels={"year": "Year", "solar_pct": "Solar %"},
            markers=True,
        )
        fig_pct.update_layout(height=350, yaxis_title="Solar Permits as % of Total")
        st.plotly_chart(fig_pct, use_container_width=True)
