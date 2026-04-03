import json
import os
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

S3_BUCKET = os.environ.get("S3_BUCKET", "expense-tracker-vardhan")
AWS_REGION = "us-east-1"
SGT = timezone(timedelta(hours=8))

app = FastAPI(title="Expense Tracker Widget API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def load_dashboard_data():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key="dashboard/latest.json")
        return json.loads(response["Body"].read())
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="Dashboard data not found in S3")
        raise HTTPException(status_code=500, detail=f"S3 error: {e}")


@app.get("/widget")
def get_widget_data():
    data = load_dashboard_data()

    today_str = datetime.now(tz=SGT).strftime("%Y-%m-%d")

    daily_spend = 0.0
    for entry in data.get("daily_spend", []):
        date_val = entry.get("date", "")
        if isinstance(date_val, str) and date_val[:10] == today_str:
            daily_spend += float(entry.get("total", 0))

    monthly_spend = float(data.get("cycle_spend", 0.0))

    top_5_merchants = [
        {
            "merchant": m.get("to_merchant", ""),
            "total": float(m.get("total", 0)),
            "count": int(m.get("count", 0)),
        }
        for m in data.get("top_merchants", [])[:5]
    ]

    return {
        "daily_spend": daily_spend,
        "monthly_spend": monthly_spend,
        "top_5_merchants": top_5_merchants,
        "cycle_start": data.get("cycle_start", ""),
        "cycle_end": data.get("cycle_end", ""),
        "generated_at": data.get("generated_at", ""),
    }
