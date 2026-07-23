"""Probe whether OSS Delta 3.2 supports liquid clustering (CLUSTER BY).

Liquid clustering is the modern successor to manual partitioning + Z-order.
We try it on a throwaway table and report success or the exact error — the
honesty rule from the project plan: document what we evaluated, don't claim
what we didn't run.

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] liquid_cluster_probe.py
"""

from __future__ import annotations

from common import build_spark

PROBE_PATH = "s3a://gold/_smoke/liquid_cluster_probe"


def main() -> None:
    spark = build_spark("liquid-cluster-probe")
    spark.sparkContext.setLogLevel("WARN")
    print(f"probing liquid clustering on delta 3.2.x at {PROBE_PATH}")

    try:
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE delta.`{PROBE_PATH}` (
                product_id STRING,
                event_time TIMESTAMP,
                price DOUBLE
            )
            USING DELTA
            CLUSTER BY (product_id, event_time)
            """
        )
    except Exception as exc:
        print("\nliquid clustering NOT supported (or not enabled) in this stack:")
        print(f"  {type(exc).__name__}: {exc}")
        print(
            "\nconclusion: stay with date partitioning + OPTIMIZE ZORDER for gold "
            "on Delta 3.2.0; revisit CLUSTER BY when upgrading Delta."
        )
        spark.stop()
        return

    print("CLUSTER BY table created successfully")

    spark.sql(
        f"""
        INSERT INTO delta.`{PROBE_PATH}` VALUES
            ('BTC-USD', timestamp '2026-07-07 00:00:00', 64000.0),
            ('ETH-USD', timestamp '2026-07-07 00:00:00', 1800.0)
        """
    )
    print("insert ok")

    spark.sql(f"OPTIMIZE delta.`{PROBE_PATH}`").show(truncate=False)
    spark.sql(f"DESCRIBE DETAIL delta.`{PROBE_PATH}`").select("numFiles", "clusteringColumns").show(
        truncate=False
    )
    print("\nliquid clustering IS supported in this stack.")

    spark.stop()


if __name__ == "__main__":
    main()
