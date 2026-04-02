# Expense Tracker

An automated personal expense tracking pipeline that ingests DBS bank transaction emails from Gmail, processes them through a lakehouse architecture on AWS S3, and surfaces insights via a Streamlit dashboard and an iPhone widget.

## Architecture

<p align="center">
  <img src="docs/architecture.svg" alt="Architecture Diagram" width="900"/>
</p>

- **Schedule**: `@hourly`
- **Lookback**: Since last successful run (falls back to 2 days), 365 days (backfill mode)
- **Deduplication**: by `email_id` against existing warehouse Parquet files

## Data Flow

1. **fetch_emails** — queries Gmail API for `ibanking.alert@dbs.com` emails
2. **load_to_lake** — saves raw email JSON to `s3://bucket/raw/{date}/`
3. **parse_emails** — extracts amount, merchant, date, type (Card/PayNow) using regex on HTML body
4. **load_to_warehouse** — writes individual Parquet files to `s3://bucket/warehouse/`, skipping duplicates
5. **generate_dashboard** — reads all warehouse Parquet files, runs DuckDB aggregations (daily spend, monthly spend, top merchants, etc.), writes `dashboard/latest.json` to S3

## Billing Cycle

All cycle-based metrics use a **15th-to-15th** billing cycle (not calendar months). For example, the March cycle runs from March 15 to April 15.

## Components

### Airflow DAG (`dags/expense_tracker.py`)

The core ETL pipeline. Runs on EC2 with Airflow, or locally on Mac via `python dags/expense_tracker.py`.

### Streamlit Dashboard (`dashboard.py`)

Web dashboard showing daily spend, monthly spend, top merchants, transaction history, and charts.

```bash
streamlit run dashboard.py --server.port 8501
```

### Widget API (`widget_api.py`)

FastAPI server that reads `dashboard/latest.json` from S3 and serves a lightweight JSON payload for the iPhone widget.

```bash
python widget_api.py    # runs on port 8051
```

**Endpoint**: `GET /widget`

```json
{
  "daily_spend": 42.50,
  "monthly_spend": 1234.00,
  "top_5_merchants": [
    {"merchant": "Grab", "total": 145.00, "count": 12}
  ],
  "cycle_start": "2026-03-15",
  "cycle_end": "2026-04-15",
  "generated_at": "2026-04-02T10:00:00"
}
```

### iPhone Widget (`ExpenseWidget.js`)

A [Scriptable](https://scriptable.app) widget that displays today's spend and current cycle spend. Tap to open the Streamlit dashboard.

**Setup**:
1. Install Scriptable on iPhone
2. Create a new script, paste the contents of `ExpenseWidget.js`
3. Add a small Scriptable widget to your home screen and select the script

## Setup

### Prerequisites

- Python 3.9+
- AWS account with S3 bucket
- Google OAuth credentials (`credentials.json`) for Gmail API access
- EC2 instance (for Airflow + serving dashboard & API)

### Installation

```bash
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file:

```
S3_BUCKET=expense-tracker-vardhan
```

### EC2 Security Groups

Open the following inbound ports:
- **8501** — Streamlit dashboard
- **8051** — Widget API
- **8080** — Airflow webserver (if needed)
