import json
import os

import boto3
import pandas as pd
import streamlit as st

S3_BUCKET = os.environ.get("S3_BUCKET", "expense-tracker-vardhan")
AWS_REGION = "ap-southeast-1"

st.set_page_config(page_title="Expense Tracker", layout="wide")
st.title("Expense Tracker Dashboard")


@st.cache_data(ttl=300)
def load_dashboard_data():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key="dashboard/latest.json")
    return json.loads(response["Body"].read())


try:
    data = load_dashboard_data()
except Exception as e:
    st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()

st.caption(f"Last updated: {data['generated_at']}")

# ── DataFrames ───────────────────────────────────────────────────────────────
df_daily = pd.DataFrame(data.get("daily_spend", []))
df_monthly = pd.DataFrame(data.get("monthly_spend", []))
df_type = pd.DataFrame(data.get("spend_by_type", []))
df_merchants = pd.DataFrame(data.get("top_merchants", []))

current_month = pd.Timestamp.now().strftime("%Y-%m")

# Total transactions this month
transactions_this_month = 0
if not df_monthly.empty and current_month in df_monthly["month"].values:
    month_row = df_monthly.loc[df_monthly["month"] == current_month]
    if "count" in month_row.columns and not month_row.empty:
        transactions_this_month = int(month_row["count"].iloc[0])

# Card / PayNow totals
card_total = 0.0
paynow_total = 0.0

if not df_type.empty:
    type_normalized = df_type.copy()
    type_normalized["type_normalized"] = type_normalized["type"].astype(str).str.strip().str.lower()

    card_match = type_normalized.loc[type_normalized["type_normalized"] == "card", "total"]
    paynow_match = type_normalized.loc[type_normalized["type_normalized"] == "paynow", "total"]

    card_total = float(card_match.iloc[0]) if not card_match.empty else 0.0
    paynow_total = float(paynow_match.iloc[0]) if not paynow_match.empty else 0.0

# Top 5 merchants
if not df_merchants.empty:
    df_merchants = df_merchants.sort_values("total", ascending=False).head(5)

# ── Top metrics ──────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Daily Spend (SGD)")
    if not df_daily.empty:
        df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce")
        df_daily = df_daily.dropna(subset=["date"]).set_index("date").sort_index()
        st.line_chart(df_daily["total"], use_container_width=True)
    else:
        st.info("No data")

with col2:
    st.subheader("Monthly Spend (SGD)")
    if not df_monthly.empty:
        st.bar_chart(df_monthly.set_index("month")["total"], use_container_width=True)
    else:
        st.info("No data")

with col3:
    st.subheader("Total Transactions This Month")
    st.metric("Transactions", transactions_this_month)

st.divider()

# ── Summary cards ────────────────────────────────────────────────────────────
card_col, paynow_col = st.columns(2)
card_col.metric("Card", f"${card_total:,.2f}")
paynow_col.metric("PayNow", f"${paynow_total:,.2f}")

st.divider()

# ── Top 5 merchants table ────────────────────────────────────────────────────
st.subheader("Top 5 Merchants")

if not df_merchants.empty:
    merchant_table = df_merchants.rename(
        columns={
            "to_merchant": "Merchant",
            "count": "Transactions",
            "total": "Total (SGD)",
        }
    )

    st.dataframe(
        merchant_table[["Merchant", "Transactions", "Total (SGD)"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No data")