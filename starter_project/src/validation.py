from __future__ import annotations

import asyncio
import csv
import json
from pathlib import Path

import aiohttp

from src.config import DISCORD_WEBHOOK_URL, OUTPUT_DIR, VALID_STATUSES


class LabValidationError(RuntimeError):
    pass


def read_rows(csv_path: str | Path) -> list[dict[str, str]]:
    with Path(csv_path).open("r", encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))


def is_positive_number(raw_amount: str) -> bool:
    try:
        return float(raw_amount) > 0
    except (TypeError, ValueError):
        return False


def build_summary(rows: list[dict[str, str]]) -> dict[str, int | str]:
    missing_customer_ids = 0
    invalid_amounts = 0
    invalid_statuses = 0

    for row in rows:
        customer_id = row.get("customer_id", "").strip()
        amount = row.get("amount", "").strip()
        status = row.get("status", "").strip()

        if not customer_id:
            missing_customer_ids += 1
        if not is_positive_number(amount):
            invalid_amounts += 1
        if status not in VALID_STATUSES:
            invalid_statuses += 1

    return {
        "row_count": len(rows),
        "missing_customer_ids": missing_customer_ids,
        "invalid_amounts": invalid_amounts,
        "invalid_statuses": invalid_statuses,
        "validation_status": (
            "failed" if missing_customer_ids or invalid_amounts or invalid_statuses else "passed"
        ),
    }


def write_summary(summary: dict[str, int | str], output_path: str | Path) -> Path:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return output_file


async def _send_discord_message_async(
    summary: dict[str, int | str],
    webhook_url: str,
) -> None:
    message = (
        f"Sales Data Quality {summary['validation_status'].upper()}\n"
        f"Rows: {summary['row_count']}\n"
        f"Missing customer_id: {summary['missing_customer_ids']}\n"
        f"Invalid amounts: {summary['invalid_amounts']}\n"
        f"Invalid statuses: {summary['invalid_statuses']}"
    )

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(webhook_url, json={"content": message}) as response:
            if response.status >= 400:
                response_text = await response.text()
                raise RuntimeError(
                    f"Discord webhook failed with status {response.status}: {response_text}"
                )


def send_discord_message(summary: dict[str, int | str], webhook_url: str = DISCORD_WEBHOOK_URL) -> None:
    if not webhook_url:
        return

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_send_discord_message_async(summary, webhook_url))
    else:
        raise RuntimeError(
            "send_discord_message() cannot be called from a running event loop. "
            "Use _send_discord_message_async() and await it instead."
        )


def run_lab_check(
    input_path: str | Path,
    output_path: str | Path | None = None,
    allow_failure: bool = False,
    skip_discord: bool = False,
) -> dict[str, int | str]:
    rows = read_rows(input_path)
    summary = build_summary(rows)
    output_file = write_summary(summary, output_path or (OUTPUT_DIR / "validation_summary.json"))

    if not skip_discord:
        send_discord_message(summary)

    if summary["validation_status"] == "failed" and not allow_failure:
        raise LabValidationError(f"Validation failed. Summary saved to {output_file}")

    return summary
