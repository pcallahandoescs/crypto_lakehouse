"""Manual backfill DAG — parameterized event-time date range.

Two Spark stages (silver then gold) match the Docker Desktop memory pattern:
separate container runs instead of one heavy JVM.

Trigger with params (UI → Trigger DAG w/ config):
  {"start_date": "2026-07-05", "end_date": "2026-07-08"}
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from lakehouse.alerts import alert_on_failure
from lakehouse.spark_compose import spark_bash_command

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="backfill_lakehouse",
    default_args=DEFAULT_ARGS,
    description="Manual bronze→silver→gold backfill for [start_date, end_date)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "backfill", "day19"],
    params={
        "start_date": Param(
            "2026-07-05",
            type="string",
            description="Inclusive UTC start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            "2026-07-08",
            type="string",
            description="Exclusive UTC end date (YYYY-MM-DD)",
        ),
    },
    doc_md=__doc__,
)
def backfill_lakehouse() -> None:
    backfill_silver = BashOperator(
        task_id="backfill_silver",
        bash_command=spark_bash_command(
            "backfill.py",
            extra_args=(
                "--start {{ params.start_date }} --end {{ params.end_date }} --skip-gold"
            ),
        ),
    )
    backfill_gold = BashOperator(
        task_id="backfill_gold",
        bash_command=spark_bash_command(
            "backfill.py",
            extra_args=(
                "--start {{ params.start_date }} --end {{ params.end_date }} --skip-silver"
            ),
        ),
    )

    backfill_silver >> backfill_gold


backfill_lakehouse()
