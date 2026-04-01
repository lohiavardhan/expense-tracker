import os
import sys
import json
import base64
import re
import io
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
import pandas as pd

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup

EXCLUDED_MERCHANTS = {
    "Coinbase A/C ending 8646",
    "IFAST FINANCIAL PL-CT SUB",
}

# DETECT ENVIRONEMNT
IS_AWS = os.path.exists('/home/ubuntu')
IS_AIRFLOW = 'airflow' in sys.modules or os.environ.get('AIRFLOW_HOME')

if IS_AWS:
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dotenv import load_dotenv
    load_dotenv('/home/ubuntu/expense-tracker/.env')

# CONFIG
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

if IS_AWS:
    TOKEN_PATH = '/home/ubuntu/expense-tracker/token.json'
    S3_BUCKET = os.environ.get('S3_BUCKET', 'expense-tracker-vardhan')
    AWS_REGION = 'ap-southeast-1'
    s3 = boto3.client('s3', region_name=AWS_REGION)
else:
    TOKEN_PATH = str(Path(__file__).resolve().parent.parent / 'token.json')
    S3_BUCKET = None

default_args = {
    'owner': 'vardhan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# HELPERS
def get_gmail_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
        else:
            raise Exception(f"No valid token at {TOKEN_PATH}")
    return build('gmail', 'v1', credentials=creds)

def get_cycle_bounds(now=None):
    now = now or datetime.now()

    if now.day >= 15:
        cycle_start = now.replace(day=15, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            next_month = now.replace(year=now.year + 1, month=1, day=1)
        else:
            next_month = now.replace(month=now.month + 1, day=1)
        cycle_end = next_month.replace(day=15, hour=0, minute=0, second=0, microsecond=0)
    else:
        if now.month == 1:
            prev_month = now.replace(year=now.year - 1, month=12, day=1)
        else:
            prev_month = now.replace(month=now.month - 1, day=1)

        cycle_start = prev_month.replace(day=15, hour=0, minute=0, second=0, microsecond=0)
        cycle_end = now.replace(day=15, hour=0, minute=0, second=0, microsecond=0)

    return cycle_start, cycle_end

def get_email_body(payload):
    if 'data' in payload.get('body', {}):
        return payload['body']['data']
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/html':
                return part['body']['data']
    return None


def get_header(headers, name):
    for h in headers:
        if h['name'] == name:
            return h['value']
    return None


def save_to_s3(key, data, content_type='application/json'):
    """Upload to S3 only if running on AWS."""
    if not IS_AWS:
        return
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
    except s3.exceptions.ClientError:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data if isinstance(data, bytes) else data.encode(),
            ContentType=content_type,
        )


def get_existing_warehouse_ids():
    """Get email_ids already in S3 warehouse to avoid duplicates."""
    if not IS_AWS:
        return set()
    existing = set()
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='warehouse/'):
            for obj in page.get('Contents', []):
                filename = obj['Key'].split('/')[-1]
                if filename.endswith('.parquet'):
                    existing.add(filename.replace('.parquet', ''))
    except Exception as e:
        print(f"Warning: could not check existing warehouse files: {e}")
    return existing


# FETCH EMAIL
def fetch_emails(**context):
    service = get_gmail_service()

    backfill = False
    if context.get('dag_run') and context['dag_run'].conf:
        backfill = context['dag_run'].conf.get('backfill', False)

    if backfill:
        since = (datetime.now() - timedelta(days=365)).strftime('%Y/%m/%d')
        print(f"Backfill mode: fetching emails since {since}")
    else:
        since = (datetime.now() - timedelta(days=2)).strftime('%Y/%m/%d')

    query = f'from:ibanking.alert@dbs.com after:{since}'

    messages = []
    page_token = None
    while True:
        kwargs = {'userId': 'me', 'q': query}
        if page_token:
            kwargs['pageToken'] = page_token
        results = service.users().messages().list(**kwargs).execute()
        messages.extend(results.get('messages', []))
        page_token = results.get('nextPageToken')
        if not page_token:
            break

    banking_emails = []
    for msg in messages:
        detail = service.users().messages().get(userId='me', id=msg['id']).execute()
        from_header = get_header(detail['payload']['headers'], 'From')
        if from_header and 'ibanking.alert@dbs.com' in from_header:
            banking_emails.append(detail)

    print(f"Fetched {len(banking_emails)} banking emails (backfill={backfill})")

    # Airflow mode: push to XCom
    if context.get('ti'):
        context['ti'].xcom_push(key='banking', value=banking_emails)

    return banking_emails


# LOAD TO LAKE
def load_to_lake(banking=None, **context):
    # Airflow mode: pull from XCom
    if banking is None and context.get('ti'):
        banking = context['ti'].xcom_pull(task_ids='fetch_emails', key='banking')

    if not banking:
        print("No emails to save to lakehouse")
        return

    if not IS_AWS:
        print(f"Skipping lakehouse save (not on AWS) — {len(banking)} emails")
        return

    date_prefix = datetime.now().strftime('%Y/%m/%d')
    saved = 0
    for detail in banking:
        save_to_s3(
            f"raw/{date_prefix}/{detail['id']}.json",
            json.dumps(detail),
        )
        saved += 1

    print(f"Saved {saved} raw emails to s3://{S3_BUCKET}/raw/{date_prefix}/")


# PARSE EMAIL
def parse_emails(banking=None, **context):
    # Airflow mode: pull from XCom
    if banking is None and context.get('ti'):
        banking = context['ti'].xcom_pull(task_ids='fetch_emails', key='banking')

    if not banking:
        print("No banking emails found to parse")
        return []

    transactions = []

    for detail in banking:
        headers = detail['payload']['headers']
        data = get_email_body(detail['payload'])
        if not data:
            continue

        html_content = base64.urlsafe_b64decode(data).decode('utf-8')
        text = BeautifulSoup(html_content, 'html.parser').get_text()

        subject = get_header(headers, 'Subject') or ''
        if 'Card' in subject:
            txn_type = 'Card'
        elif 'PayNow' in subject or 'iBanking' in subject:
            txn_type = 'PayNow'
        else:
            print(f"Skipping non-transaction email: {subject}")
            continue

        amount = re.search(r'Amount:\s*(SGD[\d,.]+)', text)
        date_match = re.search(r'Date & Time:\s*(.+?)(?:\s{2,}|\n)', text)
        to_merchant = re.search(r'To:\s*(.+?)(?:\s*\(UEN|\s*If\s|(?:\s{2,}|\n))', text)
        from_card = re.search(r'From:\s*(.+?)(?:\s{2,}|\n)', text)

        email_date_header = get_header(headers, "Date")
        email_dt = None
        if email_date_header:
            try:
                email_dt = datetime.strptime(email_date_header, "%a, %d %b %Y %H:%M:%S %z")
            except ValueError:
                email_dt = None

        raw_date = date_match.group(1).strip() if date_match else None
        parsed_date = None

        if raw_date:
            cleaned = raw_date.replace("(SGT)", "").strip()
            cleaned = " ".join(cleaned.split())

            if email_dt:
                cleaned_with_year = f"{cleaned} {email_dt.year}"
                for fmt in [
                    "%d %b %H:%M %Y",
                    "%d %B %H:%M %Y",
                    "%d/%m %H:%M %Y",
                    "%d %b %I:%M %p %Y",
                    "%d %B %I:%M %p %Y",
                ]:
                    try:
                        parsed_date = datetime.strptime(cleaned_with_year, fmt).isoformat()
                        break
                    except ValueError:
                        pass

        print(f"RAW DATE: {raw_date} | PARSED DATE: {parsed_date} | SUBJECT: {subject}")

        date_prefix = datetime.now().strftime('%Y/%m/%d')
        s3_raw_path = f"s3://{S3_BUCKET}/raw/{date_prefix}/{detail['id']}.json" if IS_AWS else None

        transactions.append({
            'email_id': detail['id'],
            'date_raw': raw_date,
            'date': parsed_date,
            'from_account': from_card.group(1).strip() if from_card else None,
            'to_merchant': to_merchant.group(1).strip() if to_merchant else None,
            'subject': subject,
            'amount': amount.group(1).strip() if amount else None,
            'type': txn_type,
            's3_raw_path': s3_raw_path,
            'created_at': datetime.now().isoformat(),
        })

    # Save processed JSON to S3
    if IS_AWS and transactions:
        date_prefix = datetime.now().strftime('%Y/%m/%d')
        run_id = datetime.now().strftime('%H%M%S')
        save_to_s3(
            f"processed/{date_prefix}/transactions_{run_id}.json",
            json.dumps(transactions),
        )

    print(f"Parsed {len(transactions)} transactions")

    # Airflow mode: push to XCom
    if context.get('ti'):
        context['ti'].xcom_push(key='transactions', value=transactions)

    return transactions


# LOAD TO WAREHOUSE
def load_to_warehouse(transactions=None, **context):
    # Airflow mode: pull from XCom
    if transactions is None and context.get('ti'):
        transactions = context['ti'].xcom_pull(task_ids='parse_emails', key='transactions')

    if not transactions:
        print("No transactions to load")
        return

    if not IS_AWS:
        # Local mode: just print results
        print("\n── Transactions ──")
        for txn in transactions:
            print(f"  {txn['date']} | {txn['amount']:>10} | {txn['type']:<7} | {txn['to_merchant']}")
        return

    # AWS mode: write Parquet to S3 warehouse
    existing_ids = get_existing_warehouse_ids()
    new_transactions = [t for t in transactions if t['email_id'] not in existing_ids]

    if not new_transactions:
        print("All transactions already in warehouse")
        return

    for txn in new_transactions:
        table = pa.table({k: [txn[k]] for k in txn})
        date_prefix = datetime.now().strftime('%Y/%m/%d')
        s3_key = f"warehouse/{date_prefix}/{txn['email_id']}.parquet"

        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        buf.seek(0)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType='application/octet-stream',
        )

    print(f"Wrote {len(new_transactions)} new transactions to warehouse")


# GENERATE DASHBOARD DATA
def generate_dashboard(**context):
    if not IS_AWS:
        print("Skipping dashboard generation (not on AWS)")
        return

    # Read all parquet files from warehouse
    paginator = s3.get_paginator('list_objects_v2')
    frames = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='warehouse/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                response = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                buf = io.BytesIO(response['Body'].read())
                frames.append(pq.read_table(buf).to_pandas())

    if not frames:
        print("No warehouse data found for dashboard")
        return

    df = pd.concat(frames, ignore_index=True)
    if "to_merchant" in df.columns:
        df = df[~df["to_merchant"].fillna("").isin(EXCLUDED_MERCHANTS)].copy()
    df["parsed_ts"] = pd.to_datetime(df["date"], errors="coerce")

    cycle_start, cycle_end = get_cycle_bounds()
    df_cycle = df[
        (df["parsed_ts"].notna()) &
        (df["parsed_ts"] >= cycle_start) &
        (df["parsed_ts"] < cycle_end)
    ].copy()

    print("Rows with parsed date:", df["date"].notna().sum() if "date" in df.columns else 0)
    print("Total rows:", len(df))

    con = duckdb.connect()

    spend_by_type = con.execute("""
        SELECT type,
               COUNT(*) AS count,
               ROUND(SUM(CAST(REPLACE(REPLACE(amount, 'SGD', ''), ',', '') AS DOUBLE)), 2) AS total
        FROM df
        GROUP BY type
        ORDER BY total DESC
    """).df()

    excluded_merchants_sql = ", ".join(f"'{m}'" for m in EXCLUDED_MERCHANTS)
    top_merchants = con.execute(f"""
        SELECT to_merchant,
            COUNT(*) AS count,
            ROUND(SUM(CAST(REPLACE(REPLACE(amount, 'SGD', ''), ',', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
        WHERE to_merchant IS NOT NULL
          AND to_merchant NOT IN ({excluded_merchants_sql})
        GROUP BY to_merchant
        ORDER BY total DESC
        LIMIT 5
    """).df()

    cycle_spend = con.execute("""
        SELECT ROUND(SUM(CAST(REPLACE(REPLACE(amount, 'SGD', ''), ',', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
    """).df()

    cycle_transactions = con.execute("""
        SELECT COUNT(*) AS count
        FROM df_cycle
    """).df()

    daily_spend = con.execute("""
        SELECT CAST(TRY_CAST(date AS TIMESTAMP) AS DATE) AS date,
            ROUND(SUM(CAST(REPLACE(REPLACE(amount, 'SGD', ''), ',', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
        WHERE TRY_CAST(date AS TIMESTAMP) IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).df()

    monthly_spend = con.execute("""
        SELECT
            CASE
                WHEN DATE_PART('day', TRY_CAST(date AS TIMESTAMP)) >= 15
                THEN STRFTIME(DATE_TRUNC('month', TRY_CAST(date AS TIMESTAMP)) + INTERVAL '14 days', '%Y-%m-%d')
                ELSE STRFTIME((DATE_TRUNC('month', TRY_CAST(date AS TIMESTAMP)) - INTERVAL '1 month') + INTERVAL '14 days', '%Y-%m-%d')
            END AS month,
            COUNT(*) AS count,
            ROUND(SUM(CAST(REPLACE(REPLACE(amount, 'SGD', ''), ',', '') AS DOUBLE)), 2) AS total
        FROM df
        WHERE TRY_CAST(date AS TIMESTAMP) IS NOT NULL
        GROUP BY month
        ORDER BY month
    """).df()

    dashboard_data = {
    'generated_at': datetime.now().isoformat(),
    'cycle_start': cycle_start.date().isoformat(),
    'cycle_end': cycle_end.date().isoformat(),
    'total_transactions': int(len(df)),
    'cycle_spend': float(cycle_spend["total"].iloc[0]) if not cycle_spend.empty and pd.notna(cycle_spend["total"].iloc[0]) else 0.0,
    'cycle_transactions': int(cycle_transactions["count"].iloc[0]) if not cycle_transactions.empty else 0,
    'spend_by_type': json.loads(spend_by_type.to_json(orient='records')),
    'top_merchants': json.loads(top_merchants.to_json(orient='records')),
    'daily_spend': json.loads(daily_spend.to_json(orient='records', date_format='iso')),
    'monthly_spend': json.loads(monthly_spend.to_json(orient='records')),
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key='dashboard/latest.json',
        Body=json.dumps(dashboard_data).encode(),
        ContentType='application/json',
    )

    print(f"Dashboard data saved: {len(df)} transactions across {len(daily_spend)} days")


# AIRFLOW DAG
if IS_AIRFLOW:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    with DAG(
        dag_id='expense_tracker',
        default_args=default_args,
        description='Track expenses: Gmail → S3 data lake → Athena warehouse',
        schedule_interval='@hourly',
        start_date=datetime(2026, 3, 29),
        catchup=False,
        tags=['expenses'],
    ) as dag:

        t1 = PythonOperator(task_id='fetch_emails', python_callable=fetch_emails)
        t_lake = PythonOperator(task_id='load_to_lake', python_callable=load_to_lake)
        t2 = PythonOperator(task_id='parse_emails', python_callable=parse_emails)
        t3 = PythonOperator(task_id='load_to_warehouse', python_callable=load_to_warehouse)
        t4 = PythonOperator(task_id='generate_dashboard', python_callable=generate_dashboard)

        t1 >> t_lake
        t1 >> t2 >> t3 >> t4


# LOCAL EXECUTION FOR MAC
if __name__ == '__main__':
    print(f"Running {'on AWS' if IS_AWS else 'locally on Mac'}")
    print(f"Token path: {TOKEN_PATH}\n")

    banking = fetch_emails()
    load_to_lake(banking)
    transactions = parse_emails(banking)
    load_to_warehouse(transactions)
    generate_dashboard()