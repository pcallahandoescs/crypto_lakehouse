"""Tests for Airflow task-failure alerting (airflow/plugins/lakehouse/alerts.py).

The callback must build a complete, structured alert from the task context and
must never raise from inside the failure path (an exception in the alert would
mask the original task failure).
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from lakehouse.alerts import _alert_payload, alert_on_failure


def _context() -> dict[str, object]:
    ti = SimpleNamespace(
        task_id="dq_validate_gold",
        try_number=2,
        max_tries=2,
        log_url="http://localhost:8088/log?task=dq_validate_gold",
    )
    dag = SimpleNamespace(dag_id="batch_lakehouse")
    return {
        "task_instance": ti,
        "dag": dag,
        "run_id": "manual__2026-07-22T00:00:00",
        "exception": ValueError("gold DQ failed: 3 checks"),
    }


def test_alert_payload_captures_task_identity_and_error() -> None:
    payload = _alert_payload(_context())
    assert payload["alert"] == "task_failed"
    assert payload["dag_id"] == "batch_lakehouse"
    assert payload["task_id"] == "dq_validate_gold"
    assert payload["run_id"] == "manual__2026-07-22T00:00:00"
    assert payload["try_number"] == 2
    assert payload["max_tries"] == 2
    assert payload["log_url"].startswith("http://")
    assert "gold DQ failed" in payload["error"]


def test_alert_payload_is_json_serializable() -> None:
    # A log drain forwards this as a single line; it must serialize cleanly.
    payload = _alert_payload(_context())
    assert json.loads(json.dumps(payload, default=str))["task_id"] == "dq_validate_gold"


def test_alert_payload_tolerates_a_sparse_context() -> None:
    payload = _alert_payload({})
    assert payload["alert"] == "task_failed"
    assert payload["dag_id"] is None
    assert payload["error"] is None


def test_alert_on_failure_logs_and_never_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # No webhook configured: it should emit the ALERT log line and return cleanly.
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with caplog.at_level(logging.ERROR, logger="lakehouse.alerts"):
        alert_on_failure(_context())

    alert_lines = [r.getMessage() for r in caplog.records if "ALERT" in r.getMessage()]
    assert len(alert_lines) == 1
    # The logged line carries the structured payload.
    _, _, payload_json = alert_lines[0].partition("ALERT ")
    assert json.loads(payload_json)["task_id"] == "dq_validate_gold"
