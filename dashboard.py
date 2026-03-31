import json
import os

import boto3
import pandas as pd
import streamlit as st

S3_BUCKET = os.environ.get('S3_BUCKET', 'expense-tracker-vardhan')
AWS_REGION = 'ap-southeast-1'

st.set_page_config(page_title='Expense Tracker', layout='wide')
st.title('Expense Tracker Dashboard')

@st.cache_data(ttl=300)
def load_dashboard_data():
    s3 = boto3.client('s3', region_name=AWS_REGION)
    response = s3.get_object(Bucket=S3_BUCKET, Key='dashboard/latest.json')
    return json.loads(response['Body'].read())

try:
    data = load_dashboard_data()
except Exception as e:
    st.error(f"Could not load dashboard data from S3: {e}")
    st.stop()

st.caption(f"Last updated: {data['generated_at']}")

# ── Top metrics ──────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

df_type = pd.DataFrame(data['spend_by_type'])
total_spend = df_type['total'].sum() if not df_type.empty else 0

df_monthly = pd.DataFrame(data.get('monthly_spend', []))
current_month = pd.Timestamp.now().strftime('%Y-%m')
if not df_monthly.empty and current_month in df_monthly['month'].values:
    this_month = df_monthly.loc[df_monthly['month'] == current_month, 'total'].iloc[0]
    prev_month = pd.Timestamp.now() - pd.DateOffset(months=1)
    prev_month_str = prev_month.strftime('%Y-%m')
    prev_total = df_monthly.loc[df_monthly['month'] == prev_month_str, 'total'].values
    delta = round(this_month - prev_total[0], 2) if len(prev_total) else None
else:
    this_month = 0
    delta = None

col1.metric('Total Transactions', data['total_transactions'])
col2.metric('Total Spend (SGD)', f"{total_spend:,.2f}")
col3.metric('This Month (SGD)', f"{this_month:,.2f}", delta=f"{delta:+.2f}" if delta is not None else None)
col4.metric('Transaction Types', len(df_type))

st.divider()

# ── Spend by type ────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader('Spend by Type')
    if not df_type.empty:
        st.bar_chart(df_type.set_index('type')['total'], use_container_width=True)
    else:
        st.info('No data')

# ── Top merchants ────────────────────────────────────────────────────────────
with col_right:
    st.subheader('Top 10 Merchants')
    df_merchants = pd.DataFrame(data['top_merchants'])
    if not df_merchants.empty:
        st.bar_chart(df_merchants.set_index('to_merchant')['total'], use_container_width=True)
    else:
        st.info('No data')

# ── Daily spend ──────────────────────────────────────────────────────────────
st.subheader('Daily Spend (SGD)')
df_daily = pd.DataFrame(data['daily_spend'])
if not df_daily.empty:
    df_daily['date'] = pd.to_datetime(df_daily['date'], errors='coerce')
    df_daily = df_daily.dropna(subset=['date']).set_index('date').sort_index()
    st.line_chart(df_daily['total'], use_container_width=True)
else:
    st.info('No data')

# ── Monthly spend ────────────────────────────────────────────────────────────
st.subheader('Monthly Spend (SGD)')
if not df_monthly.empty:
    st.bar_chart(df_monthly.set_index('month')['total'], use_container_width=True)
else:
    st.info('No data')

# ── Raw table ────────────────────────────────────────────────────────────────
with st.expander('Merchant breakdown'):
    if not df_merchants.empty:
        st.dataframe(
            df_merchants.rename(columns={'to_merchant': 'Merchant', 'count': 'Transactions', 'total': 'Total (SGD)'}),
            use_container_width=True,
            hide_index=True,
        )
