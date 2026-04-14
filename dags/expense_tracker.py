import os
import json
import base64
import re
import io
from datetime import datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import pandas as pd
from dotenv import load_dotenv

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup


from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv('/home/ubuntu/expense-tracker/.env')

EXCLUDED_MERCHANTS = {
    "Coinbase A/C ending 8646",
    "IFAST FINANCIAL PL-CT SUB",
}

VARDHAN_LOHIA_NAME = "vardhan lohia"

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
TOKEN_PATH = '/home/ubuntu/expense-tracker/token.json'
S3_BUCKET = os.environ.get('S3_BUCKET', 'expense-tracker-vardhan')
AWS_REGION = 'ap-southeast-1'
s3 = boto3.client('s3', region_name=AWS_REGION)

default_args = {
    'owner': 'vardhan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_gmail_service():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
            print("Token refreshed successfully")
        else:
            raise Exception(
                f"No valid credentials. The refresh token may have expired "
                f"(this happens when the Google Cloud app is in 'Testing' mode). "
                f"Fix: 1) Set your app to 'Production' in Google Cloud Console → "
                f"OAuth consent screen, 2) Re-generate {TOKEN_PATH} once."
            )
    elif creds.expiry and creds.expiry.replace(tzinfo=None) - datetime.now() < timedelta(minutes=10):
        creds.refresh(Request())
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
        print("Token proactively refreshed (was close to expiry)")
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
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
    except s3.exceptions.ClientError:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data if isinstance(data, bytes) else data.encode(),
            ContentType=content_type,
        )


WAREHOUSE_KEY = 'warehouse/transactions.parquet'


def get_existing_warehouse_table():
    """Download the single warehouse parquet file and return it as a PyArrow Table (or None)."""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=WAREHOUSE_KEY)
        buf = io.BytesIO(response['Body'].read())
        return pq.read_table(buf)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise
    except Exception as e:
        print(f"Warning: could not read warehouse file: {e}")
        return None


def get_existing_warehouse_ids():
    existing_table = get_existing_warehouse_table()
    if existing_table is not None and 'email_id' in existing_table.column_names:
        return set(existing_table.column('email_id').to_pylist())
    return set()


def get_last_successful_run():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key='metadata/last_successful_run.json')
        data = json.loads(response['Body'].read())
        return datetime.fromisoformat(data['timestamp'])
    except Exception:
        return None


def save_last_successful_run():
    s3.put_object(
        Bucket=S3_BUCKET,
        Key='metadata/last_successful_run.json',
        Body=json.dumps({'timestamp': datetime.now().isoformat()}).encode(),
        ContentType='application/json',
    )


def fetch_emails(**context):
    service = get_gmail_service()

    # Use Airflow's execution context dates — this makes backfill work natively.
    # data_interval_start/end are provided automatically by Airflow for every run,
    # including backfill runs. Gmail's after/before only support date granularity,
    # so hourly runs on the same day will overlap, but load_to_warehouse dedupes by email_id.
    ds_start = context.get('data_interval_start')
    ds_end = context.get('data_interval_end')

    if ds_start:
        since = (ds_start - timedelta(hours=1)).strftime('%Y/%m/%d')
    else:
        since = (datetime.now() - timedelta(days=2)).strftime('%Y/%m/%d')

    query = f'from:ibanking.alert@dbs.com after:{since}'
    if ds_end:
        before = (ds_end + timedelta(days=1)).strftime('%Y/%m/%d')
        query += f' before:{before}'

    print(f"Gmail query: {query}")

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

    print(f"Fetched {len(banking_emails)} banking emails")

    if context.get('ti'):
        context['ti'].xcom_push(key='banking', value=banking_emails)

    return banking_emails


def load_to_lake(banking=None, **context):
    if banking is None and context.get('ti'):
        banking = context['ti'].xcom_pull(task_ids='fetch_emails', key='banking')

    if not banking:
        print("No emails to save to lakehouse")
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


def parse_emails(banking=None, **context):
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
        subject_lower = subject.lower()

        # Determine transaction type and direction
        is_incoming = 'received' in subject_lower or 'credit' in subject_lower

        if 'Card' in subject:
            txn_type = 'Card'
        elif 'PayNow' in subject or 'iBanking' in subject:
            txn_type = 'PayNow'
        elif is_incoming:
            txn_type = 'Transfer'
        else:
            print(f"Skipping non-transaction email: {subject}")
            continue

        if is_incoming:
            direction = 'incoming'
            # Parse digibank incoming format:
            # "You have received SGD 200.00 via FAST transfer on 29 Mar 2026 09:44 SGT."
            # Use \s+ to handle non-breaking spaces and varying whitespace from HTML parsing
            amount = re.search(r'(SGD\s*[\d,.]+)', text)
            date_match = re.search(r'on\s+(\d+\s+\w+\s+\d{4}\s+\d+:\d+)\s+SGT', text)
            from_card = re.search(r'From:\s*(.+?)(?:\s{2,}|\n)', text)
            to_merchant = re.search(r'To:\s*(.+?)(?:\s{2,}|\n)', text)

            raw_date = date_match.group(1).strip() if date_match else None
            parsed_date = None
            if raw_date:
                for fmt in ["%d %b %Y %H:%M", "%d %B %Y %H:%M"]:
                    try:
                        parsed_date = datetime.strptime(raw_date, fmt).isoformat()
                        break
                    except ValueError:
                        pass
        else:
            direction = 'outgoing'
            amount = re.search(r'Amount:\s*(SGD[\d,.]+)', text)
            date_match = re.search(r'Date & Time:\s*(.+?)(?:\s{2,}|\n)', text)
            to_merchant = re.search(r'To:\s*(.+?)(?:\s*\(UEN|\s*If\s|(?:\s{2,}|\n))', text)
            from_card = re.search(r'From:\s*(.+?)(?:\s{2,}|\n)', text)

            # If from_account contains Vardhan Lohia, it's an incoming self-transfer
            from_account_str = from_card.group(1).strip() if from_card else ''
            if VARDHAN_LOHIA_NAME in from_account_str.lower():
                direction = 'incoming'

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

        print(f"RAW DATE: {raw_date} | PARSED DATE: {parsed_date} | DIRECTION: {direction} | SUBJECT: {subject}")

        date_prefix = datetime.now().strftime('%Y/%m/%d')
        s3_raw_path = f"s3://{S3_BUCKET}/raw/{date_prefix}/{detail['id']}.json"

        from_account_val = from_card.group(1).strip() if from_card else None
        to_merchant_val = to_merchant.group(1).strip() if to_merchant else None

        # For incoming, use the sender as the merchant so it groups with outgoing to same person
        if direction == 'incoming' and from_account_val:
            to_merchant_val = from_account_val
            # Normalize "Vardhan Lohia" to "Vardhan Lohia A/C ending 1467"
            if VARDHAN_LOHIA_NAME in from_account_val.lower():
                to_merchant_val = "Vardhan Lohia A/C ending 1467"

        transactions.append({
            'email_id': detail['id'],
            'date_raw': raw_date,
            'date': parsed_date,
            'from_account': from_account_val,
            'to_merchant': to_merchant_val,
            'subject': subject,
            'amount': amount.group(1).strip() if amount else None,
            'type': txn_type,
            'direction': direction,
            's3_raw_path': s3_raw_path,
            'created_at': datetime.now().isoformat(),
        })

    if transactions:
        date_prefix = datetime.now().strftime('%Y/%m/%d')
        run_id = datetime.now().strftime('%H%M%S')
        save_to_s3(
            f"processed/{date_prefix}/transactions_{run_id}.json",
            json.dumps(transactions),
        )

    print(f"Parsed {len(transactions)} transactions")

    if context.get('ti'):
        context['ti'].xcom_push(key='transactions', value=transactions)

    return transactions


def load_to_warehouse(transactions=None, **context):
    if transactions is None and context.get('ti'):
        transactions = context['ti'].xcom_pull(task_ids='parse_emails', key='transactions')

    if not transactions:
        print("No transactions to load")
        return

    existing_table = get_existing_warehouse_table()
    existing_ids = set()
    if existing_table is not None and 'email_id' in existing_table.column_names:
        existing_ids = set(existing_table.column('email_id').to_pylist())

    # Replace existing rows that had missing data (e.g. NULL amount) with new versions
    incoming_ids = {t['email_id'] for t in transactions}
    replace_ids = incoming_ids & existing_ids
    new_transactions = [t for t in transactions if t['email_id'] not in existing_ids or t['email_id'] in replace_ids]

    if not new_transactions:
        print("All transactions already in warehouse")
        return

    new_table = pa.table({k: [txn[k] for txn in new_transactions] for k in new_transactions[0]})

    if existing_table is not None:
        # Remove old rows that are being replaced
        if replace_ids:
            existing_df = existing_table.to_pandas()
            existing_df = existing_df[~existing_df['email_id'].isin(replace_ids)]
            existing_table = pa.Table.from_pandas(existing_df, preserve_index=False)
            print(f"Replacing {len(replace_ids)} existing rows with updated data")
        combined_table = pa.concat_tables([existing_table, new_table], promote_options='default')
    else:
        combined_table = new_table

    buf = io.BytesIO()
    pq.write_table(combined_table, buf, compression='snappy')
    buf.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=WAREHOUSE_KEY,
        Body=buf.getvalue(),
        ContentType='application/octet-stream',
    )

    print(f"Appended {len(new_transactions)} new transactions to warehouse (total: {combined_table.num_rows})")


def generate_dashboard(**context):
    existing_table = get_existing_warehouse_table()

    if existing_table is None or existing_table.num_rows == 0:
        print("No warehouse data found for dashboard")
        return

    df = existing_table.to_pandas()
    if "to_merchant" in df.columns:
        df = df[~df["to_merchant"].fillna("").isin(EXCLUDED_MERCHANTS)].copy()
    if "direction" not in df.columns:
        df["direction"] = "outgoing"
    else:
        df["direction"] = df["direction"].fillna("outgoing")
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
               ROUND(SUM((CASE WHEN direction = 'incoming' THEN -1.0 ELSE 1.0 END) * CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE)), 2) AS total
        FROM df
        GROUP BY type
        ORDER BY total DESC
    """).df()

    excluded_merchants_sql = ", ".join(f"'{m}'" for m in EXCLUDED_MERCHANTS)
    top_merchants = con.execute(f"""
        SELECT to_merchant,
            COUNT(*) AS count,
            ROUND(SUM(CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
        WHERE to_merchant IS NOT NULL
          AND to_merchant NOT IN ({excluded_merchants_sql})
          AND direction = 'outgoing'
        GROUP BY to_merchant
        ORDER BY total DESC
        LIMIT 5
    """).df()

    cycle_spend = con.execute("""
        SELECT ROUND(SUM((CASE WHEN direction = 'incoming' THEN -1.0 ELSE 1.0 END) * CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
    """).df()

    cycle_transactions = con.execute("""
        SELECT COUNT(*) AS count
        FROM df_cycle
    """).df()

    daily_spend = con.execute("""
        SELECT CAST(TRY_CAST(date AS TIMESTAMP) AS DATE) AS date,
            ROUND(SUM((CASE WHEN direction = 'incoming' THEN -1.0 ELSE 1.0 END) * CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE)), 2) AS total
        FROM df_cycle
        WHERE TRY_CAST(date AS TIMESTAMP) IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).df()

    from datetime import timezone as _tz
    today_sgt = datetime.now(tz=_tz(timedelta(hours=8))).strftime("%Y-%m-%d")
    today_transactions = con.execute(f"""
        SELECT
            STRFTIME(TRY_CAST(date AS TIMESTAMP), '%H:%M') AS time,
            CASE WHEN direction = 'incoming' THEN from_account ELSE to_merchant END AS merchant,
            direction,
            ROUND((CASE WHEN direction = 'incoming' THEN -1.0 ELSE 1.0 END) * CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE), 2) AS amount
        FROM df_cycle
        WHERE CAST(TRY_CAST(date AS TIMESTAMP) AS DATE) = '{today_sgt}'
          AND TRY_CAST(date AS TIMESTAMP) IS NOT NULL
        ORDER BY TRY_CAST(date AS TIMESTAMP) DESC
    """).df()

    monthly_spend = con.execute("""
        SELECT
            CASE
                WHEN DATE_PART('day', TRY_CAST(date AS TIMESTAMP)) >= 15
                THEN STRFTIME(DATE_TRUNC('month', TRY_CAST(date AS TIMESTAMP)) + INTERVAL '14 days', '%Y-%m-%d')
                ELSE STRFTIME((DATE_TRUNC('month', TRY_CAST(date AS TIMESTAMP)) - INTERVAL '1 month') + INTERVAL '14 days', '%Y-%m-%d')
            END AS month,
            COUNT(*) AS count,
            ROUND(SUM((CASE WHEN direction = 'incoming' THEN -1.0 ELSE 1.0 END) * CAST(REPLACE(REPLACE(REPLACE(amount, 'SGD', ''), ',', ''), ' ', '') AS DOUBLE)), 2) AS total
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
        'today_transactions': json.loads(today_transactions.to_json(orient='records')),
        'monthly_spend': json.loads(monthly_spend.to_json(orient='records')),
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key='dashboard/latest.json',
        Body=json.dumps(dashboard_data).encode(),
        ContentType='application/json',
    )

    save_last_successful_run()
    print(f"Dashboard data saved: {len(df)} transactions across {len(daily_spend)} days")



with DAG(
    dag_id='expense_tracker',
    default_args=default_args,
    description='Track expenses: Gmail -> S3 raw lake -> Parquet warehouse -> DuckDB analytics',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
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
