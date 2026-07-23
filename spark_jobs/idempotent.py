"""Idempotent Delta writes via MERGE (upsert).

Running a batch job twice must leave the table in the same state — no duplicate
rows, no partial double-writes. Delta's MERGE matches on a deterministic key:
  - matched   -> UPDATE (recompute overwrites the old version)
  - not matched -> INSERT (new key)

Uses Spark SQL (not the delta Python package) so it works with the JARs baked
into our Spark image.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()
        return True
    except Exception:
        return False


def merge_upsert(
    spark: SparkSession,
    target_path: str,
    source: DataFrame,
    merge_condition: str,
    *,
    partition_by: list[str] | None = None,
) -> None:
    """Upsert source into target on merge_condition. Creates the table if missing."""
    if not _is_delta_table(spark, target_path):
        writer = source.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
        return

    source.createOrReplaceTempView("_merge_source")
    spark.sql(
        f"""
        MERGE INTO delta.`{target_path}` AS t
        USING _merge_source AS s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_condition(keys: tuple[str, ...], alias_target: str = "t", alias_source: str = "s") -> str:
    """Build `t.key = s.key AND ...` for MERGE."""
    return " AND ".join(f"{alias_target}.{k} = {alias_source}.{k}" for k in keys)


def merge_self_update(
    spark: SparkSession,
    target_path: str,
    keys: tuple[str, ...],
    *,
    source_filter: str | None = None,
    source_limit: int | None = None,
) -> None:
    """MERGE a table into a filtered view of itself (matched rows only).

    source_filter is optional SQL (no WHERE), e.g. ``product_id = 'BTC-USD'``.
    source_limit caps source rows for local proof runs (production batch omits it).
    """
    if not _is_delta_table(spark, target_path):
        raise ValueError(f"target is not a Delta table: {target_path}")
    on = merge_condition(keys)
    if source_filter or source_limit:
        where = f"WHERE {source_filter}" if source_filter else ""
        limit = f" LIMIT {source_limit}" if source_limit else ""
        source = f"(SELECT * FROM delta.`{target_path}` {where}{limit})"
    else:
        source = f"delta.`{target_path}`"
    spark.sql(
        f"""
        MERGE INTO delta.`{target_path}` AS t
        USING {source} AS s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        """
    )
