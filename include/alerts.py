"""
Airflow failure callback that posts to a Discord webhook.
If CADENCE_DISCORD_WEBHOOK_URL is not set, the alert is logged only.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def discord_alert(context: dict) -> None:
    """
    on_failure_callback compatible with all Airflow task types.
    Sends a message to Discord with the task and run date that failed.
    """
    webhook_url = os.environ.get("CADENCE_DISCORD_WEBHOOK_URL")

    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    run_date = context.get("ds", "unknown")
    try_number = context.get("task_instance").try_number
    log_url = context.get("task_instance").log_url

    message = (
        f"**Pipeline failure** | `{dag_id}`\n"
        f"Task: `{task_id}` | Run date: `{run_date}` | Attempt: {try_number}\n"
        f"Logs: {log_url}"
    )

    log.error("CADENCE ALERT: %s", message)

    if not webhook_url:
        log.warning(
            "CADENCE_DISCORD_WEBHOOK_URL not set. Alert logged only. "
            "Set the env var to route failures to Discord."
        )
        return

    payload = json.dumps({"content": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info("Discord alert sent. Status: %s", resp.status)
    except Exception as exc:
        log.error("Failed to send Discord alert: %s", exc)
