"""Hive metastore + schema-on-read.

1) Register EXTERNAL tables over the bronze files in a Hive-style metastore.
   - The metastore stores only METADATA (table name -> columns + file location).
   - The data stays in the lake folders. DROP TABLE on an external table
     removes the metadata only, never the files.
   - Here the metastore is an embedded Derby DB in lake/_metastore. In
     production: a Hive Metastore service, AWS Glue Catalog, Unity Catalog...
2) Schema-on-read for supplier files: the structure is applied when we READ
   the raw CSV/JSON, not when it was written. The March JSON file has new
   nested fields (schema drift); we merge both schemas instead of failing.
"""
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import banner, get_spark, lake_path  # noqa: E402

BRONZE_TABLES = ["customers", "products", "stores", "orders", "order_items"]


def create_spark():
    """Try a real Hive metastore first; fall back to Spark's in-memory catalog."""
    try:
        spark = get_spark("register_tables", hive=True)
        spark.sql("SHOW DATABASES").collect()      # forces the metastore to start
        return spark, "hive (embedded Derby metastore at lake/_metastore)"
    except Exception as exc:  # pragma: no cover - only if Hive jars are missing
        print(f"  Hive metastore unavailable ({exc}); falling back to in-memory Spark catalog")
        return get_spark("register_tables", hive=False), "in-memory (fallback, not persistent)"


def register_bronze(spark):
    for db in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} COMMENT '{db} layer of the RetailCo lakehouse'")
    for t in BRONZE_TABLES:
        spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{t} USING DELTA LOCATION '{lake_path('bronze', t)}'")

    # A classic HiveQL external table over a raw CSV folder (no copy, no load).
    spark.sql("DROP TABLE IF EXISTS bronze.supplier_master")
    spark.sql(f"""
        CREATE EXTERNAL TABLE bronze.supplier_master (
            supplier_id INT, supplier_name STRING, city STRING, gstin STRING, rating DOUBLE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{lake_path('landing', 'suppliers', 'master')}'
        TBLPROPERTIES ('skip.header.line.count'='1')""")


def ingest_supplier_json(spark):
    """Schema drift: January file has 5 fields, March file adds nested ones."""
    folder = lake_path("landing", "suppliers", "deliveries")
    jan = spark.read.json(f"{folder}/deliveries_2025_01.json")
    mar = spark.read.json(f"{folder}/deliveries_2025_03.json")
    print("  January schema :", ", ".join(jan.columns))
    print("  March schema   :", ", ".join(mar.columns))
    print("  NEW in March   :", ", ".join(sorted(set(mar.columns) - set(jan.columns))))

    # Write January first, then append March with mergeSchema=true:
    # Delta adds the new columns to the table schema and January rows get NULL.
    target = lake_path("bronze", "supplier_deliveries")
    shutil.rmtree(target, ignore_errors=True) if "://" not in target else None
    jan.write.format("delta").mode("overwrite").save(target)
    try:
        mar.write.format("delta").mode("append").save(target)
    except Exception as exc:
        print("  Without mergeSchema the append FAILS:", type(exc).__name__)
    mar.write.format("delta").mode("append").option("mergeSchema", "true").save(target)
    spark.sql("DROP TABLE IF EXISTS bronze.supplier_deliveries")
    spark.sql(f"CREATE TABLE bronze.supplier_deliveries USING DELTA LOCATION '{target}'")


if __name__ == "__main__":
    spark, catalog = create_spark()
    banner(f"Registering tables   catalog = {catalog}")
    register_bronze(spark)
    ingest_supplier_json(spark)

    print("\n  Tables in the metastore:")
    for db in ("bronze",):
        for row in spark.sql(f"SHOW TABLES IN {db}").collect():
            detail = spark.sql(f"DESCRIBE TABLE EXTENDED {db}.{row.tableName}") \
                .filter("col_name in ('Type','Provider')").collect()
            info = {r.col_name: r.data_type for r in detail}
            print(f"    {db}.{row.tableName:<22} type={info.get('Type'):<9} format={info.get('Provider')}")

    print("\n  Schema-on-read query over raw CSV (HiveQL external table):")
    spark.sql("SELECT supplier_id, supplier_name, city, rating FROM bronze.supplier_master "
              "ORDER BY rating DESC LIMIT 3").show(truncate=False)
    print("  Merged supplier schema (nested struct + array after drift):")
    spark.table("bronze.supplier_deliveries").printSchema()
    spark.sql("""SELECT substr(delivered_on,1,7) AS month, count(*) AS deliveries,
                        count(transport.mode) AS with_transport_info,
                        sum(coalesce(size(batch_codes), 0)) AS batch_codes
                 FROM bronze.supplier_deliveries
                 GROUP BY 1 ORDER BY 1""").show()
    spark.stop()
