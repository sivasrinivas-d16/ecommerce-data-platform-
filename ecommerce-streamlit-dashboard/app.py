
import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
)

DATABASE = "ecommerce_data_platform"
TABLE = "agg_sales_daily"
PROFILE = "github-actions-ecommerce"
REGION = "ap-southeast-2"
S3_RESULTS = (
    "s3://ecommerce-data-platform-version1/"
    "curated/athena-results/"
)

st.title("🛒 E-Commerce Data Platform")
st.caption("Sales performance and data-quality monitoring · AWS Athena")


@st.cache_data(ttl=300)
def load_data():
    conn = connect(
        s3_staging_dir=S3_RESULTS,
        region_name=REGION,
        schema_name=DATABASE,
        work_group="primary",
        profile_name=PROFILE,
    )
    try:
        query = f"""
            SELECT
                order_date,
                sales_eligible_orders,
                sales_eligible_revenue,
                sales_eligible_units_sold,
                all_distinct_orders,
                delivered_paid_orders_before_quality_filter,
                orders_not_valid_quality,
                order_amount_mismatches,
                average_order_value
            FROM {TABLE}
            ORDER BY order_date
        """
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [item[0] for item in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        conn.close()


if st.button("🔄 Refresh data"):
    load_data.clear()
    st.rerun()

try:
    with st.spinner("Loading daily metrics from Athena..."):
        df = load_data()
except Exception as exc:
    st.error("Unable to load the Athena table.")
    st.exception(exc)
    st.stop()

if df.empty:
    st.warning("The Athena table returned no rows.")
    st.stop()

df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

numeric_columns = [
    "sales_eligible_orders",
    "sales_eligible_revenue",
    "sales_eligible_units_sold",
    "all_distinct_orders",
    "delivered_paid_orders_before_quality_filter",
    "orders_not_valid_quality",
    "order_amount_mismatches",
    "average_order_value",
]

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Date filter
valid_dates = df["order_date"].dropna()
if valid_dates.empty:
    st.error("No valid order dates were returned by Athena.")
    st.stop()

min_date = valid_dates.min().date()
max_date = valid_dates.max().date()

date_range = st.date_input(
    "Order date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

filtered = df.copy()

if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered = filtered[
        filtered["order_date"].dt.date.between(start_date, end_date)
    ]

if filtered.empty:
    st.warning("No rows found for the selected dates.")
    st.stop()

# KPI totals
eligible_orders = filtered["sales_eligible_orders"].sum()
eligible_revenue = filtered["sales_eligible_revenue"].sum()
eligible_units = filtered["sales_eligible_units_sold"].sum()
distinct_orders = filtered["all_distinct_orders"].sum()
delivered_paid = filtered[
    "delivered_paid_orders_before_quality_filter"
].sum()
quality_issues = (
    filtered["orders_not_valid_quality"].sum()
    + filtered["order_amount_mismatches"].sum()
)

# Use total revenue / eligible orders for an overall weighted AOV.
overall_aov = (
    eligible_revenue / eligible_orders
    if eligible_orders > 0
    else None
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Eligible Revenue", f"₹{eligible_revenue:,.2f}")
c2.metric("Eligible Orders", f"{eligible_orders:,.0f}")
c3.metric("Eligible Units Sold", f"{eligible_units:,.0f}")
c4.metric(
    "Average Order Value",
    f"₹{overall_aov:,.2f}" if overall_aov is not None else "N/A",
)

c5, c6 = st.columns(2)
c5.metric("All Distinct Orders", f"{distinct_orders:,.0f}")
c6.metric("Delivered & Paid Before Quality Filter", f"{delivered_paid:,.0f}")

st.divider()

# Data discrepancy warning
if delivered_paid > 0 and eligible_orders == 0:
    st.warning(
        "Data quality alert: delivered/paid orders exist, but no orders "
        "are eligible for sales reporting in this date range. Check the "
        "upstream transformation and sales-eligibility rules."
    )

st.subheader("📈 Daily Eligible Revenue")
revenue_fig = px.line(
    filtered.sort_values("order_date"),
    x="order_date",
    y="sales_eligible_revenue",
    markers=True,
    labels={
        "order_date": "Order Date",
        "sales_eligible_revenue": "Eligible Revenue (₹)",
    },
)
st.plotly_chart(revenue_fig, use_container_width=True)

left, right = st.columns(2)

with left:
    st.subheader("Eligible Orders vs Delivered/Paid Orders")
    orders_fig = px.bar(
        filtered.sort_values("order_date"),
        x="order_date",
        y=[
            "sales_eligible_orders",
            "delivered_paid_orders_before_quality_filter",
        ],
        barmode="group",
        labels={
            "order_date": "Order Date",
            "value": "Orders",
            "variable": "Metric",
        },
    )
    st.plotly_chart(orders_fig, use_container_width=True)

with right:
    st.subheader("Daily Distinct Orders")
    distinct_fig = px.line(
        filtered.sort_values("order_date"),
        x="order_date",
        y="all_distinct_orders",
        markers=True,
        labels={
            "order_date": "Order Date",
            "all_distinct_orders": "Distinct Orders",
        },
    )
    st.plotly_chart(distinct_fig, use_container_width=True)

st.subheader("📋 Daily Metrics")
st.dataframe(
    filtered.sort_values("order_date", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.subheader("🔎 Data Quality Summary")
q1, q2, q3 = st.columns(3)
q1.metric(
    "Orders Not Validated",
    f"{filtered['orders_not_valid_quality'].sum():,.0f}",
)
q2.metric(
    "Order Amount Mismatches",
    f"{filtered['order_amount_mismatches'].sum():,.0f}",
)
q3.metric("Quality Issue Count", f"{quality_issues:,.0f}")

st.caption(
    f"Source: Athena · {DATABASE}.{TABLE} · "
    f"{len(filtered):,} daily records"
)
