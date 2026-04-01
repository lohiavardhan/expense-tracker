import json
import os
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "expense-tracker-vardhan")
AWS_REGION = "ap-southeast-1"

SGT = timezone(timedelta(hours=8))

st.set_page_config(page_title="Expense Tracker", layout="wide")

title_col, date_col = st.columns([3, 1])
with title_col:
    st.title("Expense Tracker Dashboard")
with date_col:
    st.markdown(
        f"<div style='text-align:right; padding-top: 12px; font-size: 1.1rem; color: gray;'>"
        f"{datetime.now(tz=SGT).strftime('%A, %d %B %Y')}</div>",
        unsafe_allow_html=True,
    )


def get_cycle_bounds(now=None):
    now = now or datetime.now()

    if now.day >= 15:
        cycle_start = now.replace(day=15, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            cycle_end = now.replace(year=now.year + 1, month=1, day=15, hour=0, minute=0, second=0, microsecond=0)
        else:
            cycle_end = now.replace(month=now.month + 1, day=15, hour=0, minute=0, second=0, microsecond=0)
    else:
        if now.month == 1:
            cycle_start = now.replace(year=now.year - 1, month=12, day=15, hour=0, minute=0, second=0, microsecond=0)
        else:
            cycle_start = now.replace(month=now.month - 1, day=15, hour=0, minute=0, second=0, microsecond=0)

        cycle_end = now.replace(day=15, hour=0, minute=0, second=0, microsecond=0)

    return cycle_start, cycle_end


@st.cache_data(ttl=300)
def load_dashboard_data():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key="dashboard/latest.json")
    return json.loads(response["Body"].read())


try:
    data = load_dashboard_data()
except ClientError as e:
    if e.response["Error"]["Code"] == "NoSuchKey":
        st.warning("No dashboard data yet.")
    else:
        st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()
except Exception as e:
    st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()

st.caption(f"Last updated: {data['generated_at']}")

df_daily = pd.DataFrame(data.get("daily_spend", []))
df_monthly = pd.DataFrame(data.get("monthly_spend", []))
df_type = pd.DataFrame(data.get("spend_by_type", []))
df_merchants = pd.DataFrame(data.get("top_merchants", []))
df_today_txns = pd.DataFrame(data.get("today_transactions", []))

today_str = datetime.now(tz=SGT).strftime("%Y-%m-%d")
cycle_start_default, cycle_end_default = get_cycle_bounds()

cycle_start = data.get("cycle_start", cycle_start_default.date().isoformat())
cycle_end = data.get("cycle_end", cycle_end_default.date().isoformat())

daily_spend_value = 0.0
if not df_daily.empty and {"date", "total"}.issubset(df_daily.columns):
    df_daily["date"] = pd.to_datetime(df_daily["date"], errors="coerce")
    match = df_daily.loc[df_daily["date"].dt.strftime("%Y-%m-%d") == today_str, "total"]
    if not match.empty:
        daily_spend_value = float(match.sum())

monthly_spend_value = float(data.get("cycle_spend", 0.0))
transactions_this_month = int(data.get("cycle_transactions", 0))


top1, top2, top3 = st.columns(3)
top1.metric("Daily Spend", f"${daily_spend_value:,.2f}")
top2.metric("Monthly Spend", f"${monthly_spend_value:,.2f}")
top3.metric("Total Transactions This Cycle", transactions_this_month)

st.caption(f"Cycle: {cycle_start} to {cycle_end}")

st.divider()

st.subheader("Today's Transactions")
if not df_today_txns.empty:
    txn_display = df_today_txns.rename(columns={
        "time": "Time",
        "merchant": "Merchant",
        "amount": "Amount (SGD)",
    })
    st.dataframe(
        txn_display[["Time", "Merchant", "Amount (SGD)"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No transactions today")

st.divider()

st.subheader("Top 5 Merchants This Cycle")
if not df_merchants.empty:
    table_df = df_merchants.rename(
        columns={
            "to_merchant": "Merchant",
            "count": "Transactions",
            "total": "Total (SGD)",
        }
    )
    st.dataframe(
        table_df[["Merchant", "Transactions", "Total (SGD)"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No data")

st.divider()

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.subheader("Daily Spend Breakdown")
    if not df_daily.empty:
        daily_chart = df_daily.copy()
        daily_chart["date"] = pd.to_datetime(daily_chart["date"], errors="coerce")
        daily_chart = daily_chart.dropna(subset=["date"]).sort_values("date")
        daily_chart = daily_chart.set_index("date")["total"]
        daily_chart.index.name = "Date"
        st.line_chart(daily_chart, y_label="Total (SGD)")
    else:
        st.info("No data")

with bottom_right:
    st.subheader("Monthly Spend Breakdown")
    if not df_monthly.empty:
        monthly_chart = df_monthly.copy().sort_values("month")
        monthly_chart = monthly_chart.set_index("month")["total"]
        monthly_chart.index.name = "Month"
        st.bar_chart(monthly_chart, y_label="Total (SGD)")
    else:
        st.info("No data")