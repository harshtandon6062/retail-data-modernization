"""Spark Structured Streaming: Kafka topic -> parsed events -> 5-minute windows -> Delta bronze.

How it works
  * Spark reads the topic as an unbounded table; every micro-batch (here every
    10 s) it fetches new events from the last processed OFFSET of each PARTITION.
  * Offsets are saved in the CHECKPOINT folder, so after a crash/restart Spark
    continues exactly where it stopped (no gaps, no double counting in the sink).
    (Spark tracks offsets itself instead of using a Kafka consumer group commit.)
  * WATERMARK '10 minutes': wait up to 10 min for late events, then finalise a
    window and drop state for it. Later-than-that events are ignored.
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import CONFIG, KAFKA_PACKAGE, banner, get_spark, lake_path  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType  # noqa: E402

# Explicit schema = schema-on-write for the stream: bad/unknown fields can't sneak in.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType()),
])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=75, help="how long to run before stopping")
    args = ap.parse_args()
    spark = get_spark("stream_consumer", hive=False, extra_packages=[KAFKA_PACKAGE])
    banner("Structured Streaming: Kafka -> Delta (lake/bronze/clickstream)")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", CONFIG["kafka"]["bootstrap_servers"])
           .option("subscribe", CONFIG["kafka"]["topic"])
           .option("startingOffsets", "earliest")       # first run: read the topic from the start
           .load())
    # Kafka gives binary key/value + metadata (topic, partition, offset, timestamp)
    events = (raw.select(F.col("partition"), F.col("offset"),
                         F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
                 .select("partition", "offset", "e.*")
                 .withColumn("event_time", F.to_timestamp("event_time")))

    # 1) raw events -> bronze (append), one Delta table
    raw_query = (events.writeStream.format("delta").outputMode("append")
                 .option("checkpointLocation", lake_path("_checkpoints", "clickstream_raw"))
                 .trigger(processingTime="10 seconds")
                 .start(lake_path("bronze", "clickstream")))

    # 2) 5-minute tumbling window aggregation with a watermark for late data
    windowed = (events.withWatermark("event_time", "10 minutes")
                .groupBy(F.window("event_time", "5 minutes"), "event_type")
                .agg(F.count("*").alias("events"), F.round(F.sum("amount"), 2).alias("amount"),
                     F.approx_count_distinct("customer_id").alias("customers")))
    agg_query = (windowed.writeStream.format("delta").outputMode("append")   # append = only final windows
                 .option("checkpointLocation", lake_path("_checkpoints", "clickstream_5min"))
                 .trigger(processingTime="10 seconds")
                 .start(lake_path("bronze", "clickstream_5min")))
    # also show the live (still-changing) windows in the console for the demo
    console = (windowed.writeStream.format("console").outputMode("update").option("truncate", False)
               .trigger(processingTime="20 seconds").start())

    time.sleep(args.seconds)
    for q in (raw_query, agg_query, console):
        q.stop()

    df = spark.read.format("delta").load(lake_path("bronze", "clickstream"))
    print(f"\n  bronze/clickstream rows: {df.count():,}")
    print("  events per Kafka partition (offset range):")
    df.groupBy("partition").agg(F.count("*").alias("events"), F.min("offset").alias("min_offset"),
                                F.max("offset").alias("max_offset")).orderBy("partition").show()
    df.groupBy("event_type").count().orderBy("event_type").show()
    print("  checkpoint (saved offsets):", lake_path("_checkpoints", "clickstream_raw"))
    spark.stop()
