"""Sqoop-style ingestion: PostgreSQL (legacy OLTP) -> Bronze Delta tables.

WHY THIS REPLACES SQOOP
  Apache Sqoop (now retired) copied RDBMS tables into HDFS/Hive by running N
  parallel mappers, each reading one slice of the table using a split column:
      sqoop import --connect jdbc:postgresql://.../legacy_retail --table orders \
                   --split-by order_id --num-mappers 4 --target-dir /bronze/orders \
                   --incremental lastmodified --check-column updated_at --last-value '...'
  Spark's JDBC reader does the same thing:
      partitionColumn = --split-by     numPartitions = --num-mappers
      lowerBound/upperBound = the min/max Sqoop computes to cut the slices
  Spark opens numPartitions connections, each running
      SELECT ... WHERE order_id >= x AND order_id < y
  so the table is read in parallel, not through one slow connection.

INCREMENTAL / CDC
  First run: full load. Later runs: only rows with updated_at > last watermark
  (stored in lake/_state/watermarks.json). This is "query-based CDC". It cannot
  see hard DELETEs - log-based CDC (Debezium reading the Postgres WAL) can.

BRONZE RULES: keep data exactly as the source sent it (no cleaning), append-only,
plus audit columns so every row is traceable to a load.
"""
import json
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark, jdbc_props, jdbc_url, lake_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

# table -> numeric split column (Sqoop's --split-by) and number of parallel readers
TABLES = {
    "customers": ("customer_id", 2),
    "products": ("product_id", 1),
    "stores": ("store_id", 1),
    "orders": ("order_id", 4),
    "order_items": ("order_item_id", 4),
}
WATERMARK_FILE = ROOT / "lake" / "_state" / "watermarks.json"


def load_watermarks():
    return json.loads(WATERMARK_FILE.read_text()) if WATERMARK_FILE.exists() else {}


def save_watermarks(wm):
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(json.dumps(wm, indent=2))


def ingest_table(spark, table, split_col, num_partitions, watermark):
    # Push the filter down to Postgres so only changed rows cross the network.
    where = f"WHERE updated_at > TIMESTAMP '{watermark}'" if watermark else ""
    subquery = f"(SELECT * FROM {table} {where}) AS src"

    # Step 1: ask the DB for the split column's range (Sqoop does exactly this).
    bounds = spark.read.jdbc(
        jdbc_url(), f"(SELECT min({split_col}) AS lo, max({split_col}) AS hi, count(*) AS n "
                    f"FROM {table} {where}) AS b", properties=jdbc_props()).first()
    if bounds["n"] == 0:
        return 0, watermark

    # Step 2: parallel read - numPartitions slices of [lowerBound, upperBound].
    # (Bounds only decide the slice size; rows outside them are still read.)
    df = spark.read.jdbc(
        url=jdbc_url(), table=subquery, column=split_col,
        lowerBound=bounds["lo"], upperBound=bounds["hi"] + 1,
        numPartitions=num_partitions, properties={**jdbc_props(), "fetchsize": "5000"})

    # Step 3: audit columns - who/when/which batch loaded this row.
    df = (df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).replace(tzinfo=None)))
            .withColumn("_source", F.lit(f"postgres.legacy_retail.public.{table}"))
            .withColumn("_load_type", F.lit("incremental" if watermark else "full"))
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("ingest_date", F.lit(date.today().isoformat())))

    # Step 4: append to Bronze (Delta), partitioned by ingest date.
    (df.write.format("delta").mode("append").partitionBy("ingest_date")
       .option("mergeSchema", "true").save(lake_path("bronze", table)))

    new_wm = df.agg(F.max("updated_at")).first()[0]
    return bounds["n"], str(new_wm)


BATCH_ID = uuid.uuid4().hex[:8]

if __name__ == "__main__":
    spark = get_spark("ingest_jdbc", hive=False)
    watermarks = load_watermarks()
    mode = "INCREMENTAL (CDC since last watermark)" if watermarks else "FULL"
    banner(f"JDBC ingestion -> bronze   mode={mode}   batch={BATCH_ID}")
    print(f"  {'table':<12} {'split col':<14} {'readers':>7} {'rows read':>10}  watermark(updated_at)")
    for table, (split_col, n_parts) in TABLES.items():
        rows, wm = ingest_table(spark, table, split_col, n_parts, watermarks.get(table))
        watermarks[table] = wm
        print(f"  {table:<12} {split_col:<14} {n_parts:>7} {rows:>10,}  {wm}")
    save_watermarks(watermarks)
    print(f"  watermarks saved to {WATERMARK_FILE.relative_to(ROOT)}")
    spark.stop()
