"""Day 18 smoke DAG — proves the scheduler picks up code from airflow/dags/.

Real batch + backfill DAGs land in Day 19. Paused at creation by default
(AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="example_lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "day18"],
    doc_md="Smoke test that Airflow is running. Unpause and trigger manually.",
)
def example_lakehouse() -> None:
    @task
    def hello() -> str:
        return "crypto lakehouse orchestration is live"

    hello()


example_lakehouse()
