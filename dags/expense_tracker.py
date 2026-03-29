from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vardhan',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_emails():
    """Pull bank emails from Gmail API"""
    # TODO: Gmail API logic
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
