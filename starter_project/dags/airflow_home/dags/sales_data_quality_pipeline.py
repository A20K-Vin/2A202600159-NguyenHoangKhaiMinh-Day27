from __future__ import annotations

import csv
from datetime import datetime
import json
from pathlib import Path
import sys

from urllib3 import request


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    DAG = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def validate_orders_task() -> dict:
    """
    TODO:
    1. Import config values.
    2. Read the input CSV.
    3. Validate the rows.
    4. Write the JSON summary.
    5. Send the Discord alert.
    6. Raise an error on failed validation.
    """
    from src.config import (
        PASSED_DATASET,
        SUMMARY_FILE,
        VALID_STATUSES,
        DISCORD_WEBHOOK_URL,
    )

    rows = read_csv_rows(PASSED_DATASET)  # e.g. starter_project/data/orders_passed.csv

    missing_customer_ids = 0
    invalid_amounts = 0
    invalid_statuses = 0

    for row in rows:
        customer_id = row["customer_id"].strip()
        amount = row["amount"].strip()
        status = row["status"].strip()

        if not customer_id:
            missing_customer_ids += 1

        if not is_positive_number(amount):  # TODO: implement numeric check
            invalid_amounts += 1

        if status not in VALID_STATUSES:
            invalid_statuses += 1

    summary = {
        "row_count": len(rows),
        "missing_customer_ids": missing_customer_ids,
        "invalid_amounts": invalid_amounts,
        "invalid_statuses": invalid_statuses,
        "validation_status": "passed",
    }

    if missing_customer_ids or invalid_amounts or invalid_statuses:
        summary["validation_status"] = "failed"

    write_summary_json(SUMMARY_FILE, summary)  # e.g. starter_project/output/validation_summary.json
    send_discord_message(DISCORD_WEBHOOK_URL, summary)

    if summary["validation_status"] == "failed":
        raise ValueError("Validation failed. Stop the pipeline.")

    return summary

def read_csv_rows(file_path: Path) -> list[dict]:
    with Path(file_path).open("r", encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))
    
def is_positive_number(raw_amount: str) -> bool:
    try:
        return float(raw_amount) > 0
    except (TypeError, ValueError):
        return False
    
def write_summary_json(output_json_path: Path, summary: dict) -> None:
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

def send_discord_message(webhook_url: str, summary: dict) -> None:
    if not webhook_url:
        return

    message = (
        f"Sales Data Quality {summary['validation_status'].upper()}\n"
        f"Rows: {summary['row_count']}\n"
        f"Missing customer_id: {summary['missing_customer_ids']}\n"
        f"Invalid amounts: {summary['invalid_amounts']}\n"
        f"Invalid statuses: {summary['invalid_statuses']}"
    )
    payload = json.dumps({"content": message}).encode("utf-8")
    http_request = request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(http_request, timeout=15) as response:
        if response.status >= 400:
            raise RuntimeError(f"Discord webhook failed with status {response.status}")


if DAG is not None:
    with DAG(
        dag_id="sales_data_quality_pipeline",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["lab", "data-quality", "discord"],
    ) as dag:
        validate_orders = PythonOperator(
            task_id="validate_orders",
            python_callable=validate_orders_task,
        )
else:  # pragma: no cover
    dag = None
