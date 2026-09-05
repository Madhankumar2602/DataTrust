"""
DataTrust — Data Quality & Pipeline Observability Dashboard.

Run:
    streamlit run dashboard/app.py

The dashboard is read-only. It queries the existing MySQL database and
does not execute the ETL pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Make project root importable when Streamlit runs dashboard/app.py
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings


# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="DataTrust | Data Quality Observatory",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine() -> Engine:
    """Create one reusable SQLAlchemy engine for the dashboard."""
    return create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def run_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Execute a read-only SQL query and return a DataFrame."""
    engine = get_engine()

    with engine.connect() as connection:
        return pd.read_sql(
            text(query),
            connection,
            params=params,
        )


# ---------------------------------------------------------------------------
# Cached data access
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def get_pipeline_runs() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            run_id,
            pipeline_name,
            started_at,
            finished_at,
            duration_seconds,
            status,
            rows_processed,
            health_score
        FROM pipeline_runs
        ORDER BY run_id
        """
    )


@st.cache_data(ttl=60)
def get_quality_results() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            result_id,
            run_id,
            check_name,
            category,
            status,
            severity,
            affected_rows,
            affected_percentage,
            message,
            created_at
        FROM quality_results
        ORDER BY result_id
        """
    )


@st.cache_data(ttl=60)
def get_transaction_summary() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT invoice_no) AS invoices,
            COUNT(DISTINCT stock_code) AS products,
            COUNT(DISTINCT country) AS countries,
            SUM(revenue) AS total_revenue,
            SUM(CASE WHEN is_cancellation = 1 THEN 1 ELSE 0 END)
                AS cancellation_rows
        FROM retail_transactions
        """
    )


@st.cache_data(ttl=300)
def get_monthly_revenue() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            DATE_FORMAT(invoice_date, '%Y-%m-01') AS month,
            SUM(revenue) AS revenue
        FROM retail_transactions
        GROUP BY DATE_FORMAT(invoice_date, '%Y-%m-01')
        ORDER BY month
        """
    )


@st.cache_data(ttl=300)
def get_top_products() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            stock_code,
            MAX(description) AS description,
            SUM(quantity) AS quantity,
            SUM(revenue) AS revenue
        FROM retail_transactions
        GROUP BY stock_code
        ORDER BY revenue DESC
        LIMIT 10
        """
    )


@st.cache_data(ttl=300)
def get_country_revenue() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            country,
            SUM(revenue) AS revenue
        FROM retail_transactions
        WHERE country IS NOT NULL
        GROUP BY country
        ORDER BY revenue DESC
        LIMIT 15
        """
    )


@st.cache_data(ttl=60)
def get_anomalies() -> pd.DataFrame:
    return run_query(
        """
        SELECT
            anomaly_id,
            metric,
            period,
            value,
            expected_value,
            deviation_pct,
            severity,
            message,
            detected_at
        FROM anomaly_results
        ORDER BY anomaly_id DESC
        LIMIT 200
        """
    )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# Pipeline EXECUTION statuses that mean the run finished normally. The writers
# use two spellings: src/etl/pipeline.py and the Airflow DAG store "SUCCESS",
# while QualityRepository.save_run defaults to "COMPLETED" (used by
# run_database.py). Data-quality findings are reported separately and never
# make a run "failed".
PIPELINE_SUCCESS_STATUSES = {"SUCCESS", "COMPLETED"}


def health_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 50:
        return "Poor"
    return "Critical"


def format_number(value: float | int) -> str:
    return f"{value:,.0f}"


def format_currency(value: float | int) -> str:
    return f"£{value:,.2f}"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("🛡️ DataTrust")

st.sidebar.markdown(
    """
### Data Quality Observatory

Monitor:

- Pipeline health
- Data-quality failures
- Quality warnings
- ETL performance
- Retail analytics
"""
)

page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "Data Quality",
        "Retail Analytics",
        "Pipeline History",
        "Anomalies",
    ],
)

st.sidebar.divider()

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption("Data source: MySQL")
st.sidebar.caption("Dashboard is read-only")


# ---------------------------------------------------------------------------
# Load core data
# ---------------------------------------------------------------------------

try:
    pipeline_runs = get_pipeline_runs()
    quality_results = get_quality_results()
    transaction_summary = get_transaction_summary()
except Exception as exc:
    st.error("Unable to connect to the DataTrust MySQL database.")
    st.exception(exc)
    st.stop()


if pipeline_runs.empty:
    st.warning("No pipeline runs have been stored yet.")
    st.stop()


latest_run = pipeline_runs.iloc[-1]

latest_score = float(latest_run["health_score"])
latest_status = str(latest_run["status"])
rows_processed = int(latest_run["rows_processed"])

failed_checks = quality_results[
    (quality_results["run_id"] == latest_run["run_id"])
    & (quality_results["status"] == "FAIL")
]

warning_checks = quality_results[
    (quality_results["run_id"] == latest_run["run_id"])
    & (quality_results["status"] == "WARNING")
]


# ===========================================================================
# OVERVIEW
# ===========================================================================

if page == "Overview":

    st.title("🛡️ DataTrust")
    st.caption("Data Quality & Pipeline Observability Platform")

    # -----------------------------------------------------------------------
    # Run comparison
    # -----------------------------------------------------------------------

    latest_run = pipeline_runs.iloc[-1]

    if len(pipeline_runs) >= 2:
        previous_run = pipeline_runs.iloc[-2]
        score_delta = latest_score - float(previous_run["health_score"])
    else:
        previous_run = None
        score_delta = None

    # -----------------------------------------------------------------------
    # Header status
    # -----------------------------------------------------------------------

    status_col, score_col = st.columns([1, 3])

    with status_col:
        if latest_status in PIPELINE_SUCCESS_STATUSES:
            st.success(f"● PIPELINE {latest_status}")
        elif latest_status == "WARNING":
            st.warning("● PIPELINE WARNING")
        else:
            st.error(f"● PIPELINE {latest_status or 'UNKNOWN'}")

    with score_col:
        if score_delta is None:
            st.caption("First recorded pipeline run")
        else:
            direction = "↑" if score_delta > 0 else "↓" if score_delta < 0 else "→"
            st.caption(
                f"Health score change: {direction} "
                f"{abs(score_delta):.2f} points vs previous run"
            )

    # -----------------------------------------------------------------------
    # KPI cards
    # -----------------------------------------------------------------------

    st.divider()

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Data Health",
            f"{latest_score:.2f}/100",
            health_label(latest_score),
        )

    with col2:
        st.metric(
            "Rows Processed",
            format_number(rows_processed),
        )

    with col3:
        st.metric(
            "Failures",
            len(failed_checks),
        )

    with col4:
        st.metric(
            "Warnings",
            len(warning_checks),
        )

    with col5:
        st.metric(
            "Duration",
            f"{float(latest_run['duration_seconds']):.1f}s",
        )

    # -----------------------------------------------------------------------
    # Attention panel
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("🚨 What Needs Attention?")

    attention_left, attention_right = st.columns(2)

    with attention_left:

        if failed_checks.empty:
            st.success("No failed quality checks.")
        else:
            st.error(f"{len(failed_checks)} quality check(s) failed.")

            for _, row in failed_checks.iterrows():
                st.markdown(
                    f"**{row['check_name']}**  \n"
                    f"{row['message']}"
                )

    with attention_right:

        if warning_checks.empty:
            st.success("No quality warnings.")
        else:
            st.warning(f"{len(warning_checks)} warning(s) detected.")

            for _, row in warning_checks.iterrows():
                st.markdown(
                    f"**{row['check_name']}**  \n"
                    f"{row['message']}"
                )

    # -----------------------------------------------------------------------
    # Health trend
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("📈 Data Health Trend")

    trend = pipeline_runs.copy()
    trend["started_at"] = pd.to_datetime(trend["started_at"])

    fig = px.line(
        trend,
        x="started_at",
        y="health_score",
        markers=True,
        labels={
            "started_at": "Pipeline Run",
            "health_score": "Health Score",
        },
    )

    fig.update_yaxes(range=[0, 100])

    fig.add_hline(
        y=90,
        line_dash="dash",
        annotation_text="Warning",
    )

    fig.add_hline(
        y=75,
        line_dash="dash",
        annotation_text="Good",
    )

    fig.update_layout(
        height=420,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # -----------------------------------------------------------------------
    # Latest run + quality distribution
    # -----------------------------------------------------------------------

    left, right = st.columns(2)

    with left:

        st.subheader("⚙️ Latest Pipeline Run")

        pipeline_info = pd.DataFrame(
            {
                "Metric": [
                    "Run ID",
                    "Pipeline",
                    "Status",
                    "Rows Processed",
                    "Duration",
                    "Health Score",
                ],
                "Value": [
                    str(latest_run["run_id"]),
                    str(latest_run["pipeline_name"]),
                    latest_status,
                    format_number(rows_processed),
                    f"{float(latest_run['duration_seconds']):.2f} sec",
                    f"{latest_score:.2f}/100",
                ],
            }
        )

        st.dataframe(
            pipeline_info,
            hide_index=True,
            use_container_width=True,
        )

    with right:

        st.subheader("🔎 Quality Check Distribution")

        latest_quality = quality_results[
            quality_results["run_id"] == latest_run["run_id"]
        ]

        if latest_quality.empty:

            st.info("No quality results available.")

        else:

            status_counts = (
                latest_quality["status"]
                .value_counts()
                .rename_axis("status")
                .reset_index(name="count")
            )

            fig = px.pie(
                status_counts,
                names="status",
                values="count",
                hole=0.55,
            )

            fig.update_layout(
                height=350,
                margin=dict(l=10, r=10, t=30, b=10),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    # -----------------------------------------------------------------------
    # Retail snapshot
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("🛒 Retail Data Snapshot")

    summary = transaction_summary.iloc[0]

    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.metric(
            "Transactions",
            format_number(summary["total_rows"]),
        )

    with c2:
        st.metric(
            "Invoices",
            format_number(summary["invoices"]),
        )

    with c3:
        st.metric(
            "Products",
            format_number(summary["products"]),
        )

    with c4:
        st.metric(
            "Countries",
            format_number(summary["countries"]),
        )

    with c5:
        st.metric(
            "Revenue",
            format_currency(summary["total_revenue"]),
        )

# ===========================================================================
# DATA QUALITY
# ===========================================================================

elif page == "Data Quality":

    st.title("🔎 Data Quality")

    st.markdown(
        "Detailed quality findings produced by the DataTrust validation engine."
    )

    latest_quality = quality_results[
        quality_results["run_id"] == latest_run["run_id"]
    ].copy()

    # Summary cards
    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Total Checks",
            len(latest_quality),
        )

    with c2:
        st.metric(
            "Failures",
            len(latest_quality[latest_quality["status"] == "FAIL"]),
        )

    with c3:
        st.metric(
            "Warnings",
            len(latest_quality[latest_quality["status"] == "WARNING"]),
        )

    st.divider()

    # Category breakdown
    st.subheader("Quality by Dimension")

    category_summary = (
        latest_quality
        .groupby(["category", "status"])
        .size()
        .reset_index(name="checks")
    )

    if not category_summary.empty:

        fig = px.bar(
            category_summary,
            x="category",
            y="checks",
            color="status",
            barmode="group",
            labels={
                "category": "Quality Dimension",
                "checks": "Number of Checks",
                "status": "Status",
            },
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    st.divider()

    # Detailed results
    st.subheader("Detailed Validation Results")

    display_columns = [
        "check_name",
        "category",
        "status",
        "severity",
        "affected_rows",
        "affected_percentage",
        "message",
    ]

    available_columns = [
        column
        for column in display_columns
        if column in latest_quality.columns
    ]

    st.dataframe(
        latest_quality[available_columns],
        hide_index=True,
        use_container_width=True,
    )

    st.divider()

    # Failures
    st.subheader("🚨 Failed Checks")

    failures = latest_quality[
        latest_quality["status"] == "FAIL"
    ]

    if failures.empty:
        st.success("No failed quality checks.")
    else:
        for _, row in failures.iterrows():
            st.error(
                f"**{row['check_name']}** — "
                f"{row['message']}"
            )

    # Warnings
    st.subheader("⚠️ Warnings")

    warnings = latest_quality[
        latest_quality["status"] == "WARNING"
    ]

    if warnings.empty:
        st.success("No warnings.")
    else:
        for _, row in warnings.iterrows():
            st.warning(
                f"**{row['check_name']}** — "
                f"{row['message']}"
            )


# ===========================================================================
# RETAIL ANALYTICS
# ===========================================================================

elif page == "Retail Analytics":

    st.title("📊 Retail Analytics")

    st.markdown(
        "Business analysis from the transformed MySQL transaction snapshot."
    )

    summary = transaction_summary.iloc[0]

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Total Revenue",
            format_currency(summary["total_revenue"]),
        )

    with c2:
        st.metric(
            "Products",
            format_number(summary["products"]),
        )

    with c3:
        cancellation_rate = (
            float(summary["cancellation_rows"])
            / float(summary["total_rows"])
            * 100
            if summary["total_rows"]
            else 0
        )

        st.metric(
            "Cancellation Rate",
            f"{cancellation_rate:.2f}%",
        )

    st.divider()

    # Monthly revenue
    st.subheader("💰 Monthly Revenue")

    monthly = get_monthly_revenue()

    if not monthly.empty:

        monthly["month"] = pd.to_datetime(monthly["month"])

        fig = px.line(
            monthly,
            x="month",
            y="revenue",
            markers=True,
            labels={
                "month": "Month",
                "revenue": "Revenue",
            },
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

    # Top products
    left, right = st.columns(2)

    with left:

        st.subheader("🏆 Top Products")

        products = get_top_products()

        if not products.empty:

            products["label"] = (
                products["stock_code"].astype(str)
                + " — "
                + products["description"].fillna("Unknown")
            )

            fig = px.bar(
                products.sort_values("revenue"),
                x="revenue",
                y="label",
                orientation="h",
                labels={
                    "revenue": "Revenue",
                    "label": "Product",
                },
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

    with right:

        st.subheader("🌍 Revenue by Country")

        countries = get_country_revenue()

        if not countries.empty:

            fig = px.bar(
                countries.sort_values("revenue"),
                x="revenue",
                y="country",
                orientation="h",
                labels={
                    "revenue": "Revenue",
                    "country": "Country",
                },
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )


# ===========================================================================
# PIPELINE HISTORY
# ===========================================================================

elif page == "Pipeline History":

    st.title("⚙️ Pipeline History")

    st.markdown(
        "Historical execution and health information stored by DataTrust."
    )

    # Historical table
    st.subheader("Pipeline Runs")

    history = pipeline_runs.copy()

    history["started_at"] = pd.to_datetime(
        history["started_at"]
    )

    st.dataframe(
        history[
            [
                "run_id",
                "pipeline_name",
                "started_at",
                "status",
                "rows_processed",
                "duration_seconds",
                "health_score",
            ]
        ],
        hide_index=True,
        use_container_width=True,
    )

    st.divider()

    # Health score trend
    st.subheader("📈 Historical Health Score")

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=history["run_id"],
            y=history["health_score"],
            mode="lines+markers",
            name="Health Score",
            text=[
                f"Run {run_id}"
                for run_id in history["run_id"]
            ],
            hovertemplate=(
                "%{text}<br>"
                "Health Score: %{y:.2f}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        xaxis_title="Pipeline Run",
        yaxis_title="Health Score",
        yaxis=dict(range=[0, 100]),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # Duration
    st.subheader("⏱️ Pipeline Duration")

    fig = px.bar(
        history,
        x="run_id",
        y="duration_seconds",
        text_auto=".2f",
        labels={
            "run_id": "Pipeline Run",
            "duration_seconds": "Duration (seconds)",
        },
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    # Rows processed
    st.subheader("📦 Rows Processed")

    fig = px.bar(
        history,
        x="run_id",
        y="rows_processed",
        text_auto=".0f",
        labels={
            "run_id": "Pipeline Run",
            "rows_processed": "Rows",
        },
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )


# ===========================================================================
# ANOMALIES
# ===========================================================================

elif page == "Anomalies":

    st.title("⚠️ Anomaly Detection")

    st.markdown(
        "Statistical anomalies detected by DataTrust across revenue, "
        "transaction volume, and cancellation rate. All values are "
        "sourced live from MySQL — nothing is hardcoded."
    )

    try:
        anomalies_df = get_anomalies()
    except Exception as exc:
        st.error("Unable to load anomaly data.")
        st.exception(exc)
        st.stop()

    if anomalies_df.empty:
        st.info(
            "No anomalies have been persisted yet. "
            "Run `run_anomaly.py` to detect and store anomalies."
        )
        st.stop()

    # -----------------------------------------------------------------------
    # KPI cards
    # -----------------------------------------------------------------------

    st.divider()

    n_critical = int((anomalies_df["severity"] == "CRITICAL").sum())
    n_warning = int((anomalies_df["severity"] == "WARNING").sum())
    n_metrics = anomalies_df["metric"].nunique()
    n_periods = anomalies_df["period"].nunique()

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.metric("🔴 Critical Anomalies", n_critical)
    with k2:
        st.metric("🟡 Warnings", n_warning)
    with k3:
        st.metric("Metrics Affected", n_metrics)
    with k4:
        st.metric("Periods Affected", n_periods)

    # -----------------------------------------------------------------------
    # Filters
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("🔍 Filter Anomalies")

    filter_col1, filter_col2 = st.columns(2)

    with filter_col1:
        severity_options = ["All"] + sorted(anomalies_df["severity"].unique().tolist())
        selected_severity = st.selectbox(
            "Severity",
            severity_options,
            key="anomaly_severity_filter",
        )

    with filter_col2:
        metric_options = ["All"] + sorted(anomalies_df["metric"].unique().tolist())
        selected_metric = st.selectbox(
            "Metric",
            metric_options,
            key="anomaly_metric_filter",
        )

    filtered = anomalies_df.copy()

    if selected_severity != "All":
        filtered = filtered[filtered["severity"] == selected_severity]

    if selected_metric != "All":
        filtered = filtered[filtered["metric"] == selected_metric]

    st.caption(f"Showing {len(filtered)} of {len(anomalies_df)} anomalies")

    # -----------------------------------------------------------------------
    # Anomaly alert cards
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("🚨 Anomaly Summary")

    for _, row in filtered.iterrows():
        direction = "↑" if row["deviation_pct"] > 0 else "↓"
        pct = abs(float(row["deviation_pct"]))
        label = (
            f"{row['severity']} | {row['metric'].title()} | "
            f"{row['period']} | {direction}{pct:.1f}%"
        )
        detail = (
            f"**Value:** {float(row['value']):,.2f}  \n"
            f"**Expected:** {float(row['expected_value']):,.2f}  \n"
            f"**Message:** {row['message']}"
        )
        if row["severity"] == "CRITICAL":
            with st.expander(f"🔴 {label}"):
                st.markdown(detail)
        else:
            with st.expander(f"🟡 {label}"):
                st.markdown(detail)

    # -----------------------------------------------------------------------
    # Deviation chart
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("📊 Deviation % by Anomaly")

    if not filtered.empty:
        chart_df = filtered.copy()
        chart_df["label"] = (
            chart_df["metric"].str.title()
            + " "
            + chart_df["period"]
        )
        chart_df["abs_deviation"] = chart_df["deviation_pct"].abs()

        fig = px.bar(
            chart_df.sort_values("abs_deviation"),
            x="abs_deviation",
            y="label",
            color="severity",
            orientation="h",
            color_discrete_map={
                "CRITICAL": "#EF4444",
                "WARNING": "#F59E0B",
            },
            labels={
                "abs_deviation": "Absolute Deviation (%)",
                "label": "Anomaly",
                "severity": "Severity",
            },
        )

        fig.update_layout(
            height=max(300, 80 * len(chart_df)),
            margin=dict(l=10, r=20, t=30, b=20),
        )

        st.plotly_chart(fig, use_container_width=True)

    # -----------------------------------------------------------------------
    # Historical anomaly table
    # -----------------------------------------------------------------------

    st.divider()
    st.subheader("📋 Anomaly History Table")

    display_cols = [
        "severity",
        "metric",
        "period",
        "value",
        "expected_value",
        "deviation_pct",
        "message",
        "detected_at",
    ]

    available = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[available].rename(columns={
            "expected_value": "expected",
            "deviation_pct": "deviation %",
            "detected_at": "detected at",
        }),
        hide_index=True,
        use_container_width=True,
    )
