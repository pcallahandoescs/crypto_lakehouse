"""Task-failure alerting for lakehouse DAGs.

Airflow already retries and records state; alerting adds the *push* — a single
structured line that a log drain (Slack/email/PagerDuty) can match and forward.
Locally it emits a JSON ``ALERT`` record to the task log; the ``SLACK_WEBHOOK_URL``
hook shows exactly where a real integration would slot in.

Wire it via ``default_args={"on_failure_callback": alert_on_failure}``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib import request

logger = logging.getLogger("lakehouse.alerts")


def _alert_payload(context: dict[str, Any]) -> dict[str, Any]:
    ti = context.get("task_instance")
    dag = context.get("dag")
    exc = context.get("exception")
    return {
        "alert": "task_failed",
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "try_number": getattr(ti, "try_number", None),
        "max_tries": getattr(ti, "max_tries", None),
        "log_url": getattr(ti, "log_url", None),
        "error": str(exc) if exc else None,
    }


def _post_to_slack(webhook_url: str, payload: dict[str, Any]) -> None:
    text = (
        f":rotating_light: *{payload['dag_id']}.{payload['task_id']}* failed "
        f"(try {payload['try_number']}/{payload['max_tries']})\n"
        f"{payload['error'] or 'see logs'}\n{payload['log_url'] or ''}"
    )
    body = json.dumps({"text": text}).encode()
    req = request.Request(
        webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    request.urlopen(req, timeout=5)  # noqa: S310 - trusted, operator-configured URL


def alert_on_failure(context: dict[str, Any]) -> None:
    """on_failure_callback: emit a structured alert; optionally push to Slack."""
    payload = _alert_payload(context)
    logger.error("ALERT %s", json.dumps(payload, default=str))

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        try:
            _post_to_slack(webhook_url, payload)
        except Exception as err:  # pragma: no cover - never fail the callback
            logger.warning("slack_alert_failed: %s", err)
