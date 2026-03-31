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

# Data
df_daily = pd.DataFrame(data.get("daily_spend", []))
df_monthly = pd.DataFrame(data.get("monthly_spend", []))
df_type = pd.DataFrame(data.get("spend_by_type", []))
df_merchants = pd.DataFrame(data.get("top_merchants", []))

current_month = pd.Timestamp.now().strftime("%Y-%m")

# total transactions this month
transactions_this_month = 0
if not df_monthly.empty and "month" in df_monthly.columns and "count" in df_monthly.columns:
    match = df_monthly.loc[df_monthly["month"] == current_month, "count"]
    if not match.empty:
        transactions_this_month = int(match.iloc[0])

# card / paynow totals
card_total = 0.0
paynow_total = 0.0

if not df_type.empty and "type" in df_type.columns and "total" in df_type.columns:
    tmp = df_type.copy()
    tmp["type_norm"] = tmp["type"].astype(str).str.strip().str.lower()

    card_match = tmp.loc[tmp["type_norm"] == "card", "total"]
    paynow_match = tmp.loc[tmp["type_norm"] == "paynow", "total"]

    if not card_match.empty:
        card_total = float(card_match.iloc[0])
    if not paynow_match.empty:
        paynow_total = float(paynow_match.iloc[0])

# top 5 merchants
if not df_merchants.empty and "total" in df_merchants.columns:
    df_merchants = df_merchants.sort_values("total", ascending=False).head(5)

# charts row
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Daily Spend (SGD)")
    if not df_daily.empty:
        # support either "date" or "day" from backend
        date_col = "date" if "date" in df_daily.columns else "day" if "day" in df_daily.columns else None

        if date_col:
            df_daily[date_col] = pd.to_datetime(df_daily[date_col], errors="coerce")
            df_daily = df_daily.dropna(subset=[date_col]).set_index(date_col).sort_index()
            if "total" in df_daily.columns and not df_daily.empty:
                st.line_chart(df_daily["total"], use_container_width=True)
            else:
                st.info("No data")
        else:
            st.info("No data")
    else:
        st.info("No data")

with col2:
    st.subheader("Monthly Spend (SGD)")
    if not df_monthly.empty and {"month", "total"}.issubset(df_monthly.columns):
        st.bar_chart(df_monthly.set_index("month")["total"], use_container_width=True)
    else:
        st.info("No data")

with col3:
    st.subheader("Total Transactions This Month")
    st.metric("Transactions", transactions_this_month)

st.divider()

# summary cards
c1, c2 = st.columns(2)
c1.metric("Card", f"${card_total:,.2f}")
c2.metric("PayNow", f"${paynow_total:,.2f}")

st.divider()

# top 5 merchants table
st.subheader("Top 5 Merchants")
if not df_merchants.empty:
    table_df = df_merchants.rename(
        columns={
            "to_merchant": "Merchant",
            "count": "Transactions",
            "total": "Total (SGD)",
        }
    )

    cols = [c for c in ["Merchant", "Transactions", "Total (SGD)"] if c in table_df.columns]
    st.dataframe(table_df[cols], use_container_width=True, hide_index=True)
else:
    st.info("No data")