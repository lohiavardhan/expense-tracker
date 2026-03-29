from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pickle
import os.path
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

default_args = {
    'owner': 'vardhan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_gmail_service():
    creds = None
    # token.json stores the user's access/refresh tokens after first auth
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # If no valid creds, do the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def fetch_emails():
    """Pull all emails from Gmail API"""

    service = get_gmail_service()
    results = service.users().messages().list(userId='me', q='newer_than:1h').execute()
    messages = results.get('messages', [])
    
    print("Fetching emails...")
    

def parse_transactions(**context):
    """Filter bank emails and extract amount, merchant, date"""
    # TODO: Parse email HTML/text
    print("Parsing transactions...")

def categorize(**context):
    """Assign category based on merchant name"""
    # TODO: Keyword matching logic
    print("Categorizing expenses...")

def load_to_db(**context):
    """Insert cleaned transactions into PostgreSQL"""
    # TODO: Database insert
    print("Loading to database...")

with DAG(
    dag_id='expense_tracker',
    default_args=default_args,
    description='Track expenses from bank emails',
    schedule_interval='@hourly',
    start_date=datetime(2026, 3, 29),
    catchup=False,
    tags=['expenses'],
) as dag:

    t1 = PythonOperator(task_id='fetch_emails', python_callable=fetch_emails)
    t2 = PythonOperator(task_id='parse_transactions', python_callable=parse_transactions)
    t3 = PythonOperator(task_id='categorize', python_callable=categorize)
    t4 = PythonOperator(task_id='load_to_db', python_callable=load_to_db)

    t1 >> t2 >> t3 >> t4
