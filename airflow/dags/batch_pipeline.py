"""Day 19: scheduled batch pipeline — gold → DQ.

Silver is kept current by the **streaming** silver_transform job (Lambda speed/batch
split). Gold runs **one product per Spark container**, **sequentially** — parallel runs OOM
on Docker Desktop when BTC + ETH compete for RAM (~4 GB each).

Manual trigger: unpause → Trigger. Expect several minutes per Spark stage.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from lakehouse.spark_compose import spark_bash_command

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="batch_lakehouse",
    default_args=DEFAULT_ARGS,
    description="Nightly gold per product → DQ (silver maintained by streaming)",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "batch", "day19"],
    doc_md=__doc__,
)
def batch_lakehouse() -> None:
    gold_btc = BashOperator(
        task_id="gold_aggregate_btc",
        bash_command=spark_bash_command(
            "gold_aggregate.py",
            extra_env={"GOLD_PRODUCT_ID": "BTC-USD"},
        ),
        execution_timeout=timedelta(minutes=30),
    )
    gold_eth = BashOperator(
        task_id="gold_aggregate_eth",
        bash_command=spark_bash_command(
            "gold_aggregate.py",
            extra_env={"GOLD_PRODUCT_ID": "ETH-USD"},
        ),
        execution_timeout=timedelta(minutes=30),
    )
    dq_silver = BashOperator(
        task_id="dq_validate_silver",
        bash_command=spark_bash_command(
            "dq_validate.py",
            extra_env={"DQ_LAYER": "silver"},
        ),
        execution_timeout=timedelta(minutes=15),
    )
    dq_gold = BashOperator(
        task_id="dq_validate_gold",
        bash_command=spark_bash_command(
            "dq_validate.py",
            extra_env={"DQ_LAYER": "gold"},
        ),
        execution_timeout=timedelta(minutes=15),
    )

    gold_btc >> gold_eth >> dq_silver >> dq_gold


batch_lakehouse()
