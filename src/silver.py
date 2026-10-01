"""Silver layer: clean, dedupe, standardise, type-cast, mask PII, quarantine bad rows.

Bronze keeps EVERY version of every row (full load + each CDC batch, plus the
legacy system's own duplicates). Silver = one clean, current row per key.

Rule of thumb used here: never silently drop data. A row that breaks a rule
goes to lake/quarantine/<table>/ with a `_reject_reason`, so a data steward
can fix it at the source and the numbers still add up in reconciliation.

The small functions at the top are pure DataFrame -> DataFrame functions so
they can be unit tested (see tests/).
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark, lake_path  # noqa: E402

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


# ---------- reusable building blocks (unit tested) ----------

def dedupe_latest(df: DataFrame, key: str) -> DataFrame:
    """Keep only the newest version of each key.
    Handles both CDC history (same key, newer updated_at) and exact duplicates
    (same key, same updated_at) -> row_number() picks exactly one row."""
    w = Window.partitionBy(key).orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def mask_email(col):
    """SHA-256 of the normalised email: analysts can still COUNT DISTINCT or join
    on it, but cannot read the address. (Hashing = pseudonymisation.)"""
    return F.when(col.isNull(), None).otherwise(F.sha2(F.lower(F.trim(col)), 256))


def normalise_phone(col):
    """'+91-98xxxxxxx', '098...', '+91 98xxx xxxxx' -> last 10 digits."""
    digits = F.regexp_replace(col, r"[^0-9]", "")
    return F.when(F.length(digits) >= 10, F.substring(digits, -10, 10))


def mask_phone(col):
    """Show only the last 4 digits: 9876543210 -> XXXXXX3210."""
    p = normalise_phone(col)
    return F.when(p.isNull(), None).otherwise(F.concat(F.lit("XXXXXX"), F.substring(p, -4, 4)))


def standardise_city(col):
    """' mumbai', 'MUMBAI ' -> 'Mumbai'."""
    return F.initcap(F.lower(F.trim(col)))


def parse_legacy_date(col):
    """The legacy app stored dates as text in two formats. Try each; NULL if neither fits.
    try_to_timestamp returns NULL instead of failing on '2025-02-30' or 'N/A'."""
    return F.coalesce(F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd HH:mm:ss')"),
                      F.expr(f"try_to_timestamp({col}, 'dd/MM/yyyy HH:mm')"))


def split_valid(df: DataFrame, rules):
    """rules = [(condition_that_means_BAD, reason_text), ...]
    Returns (valid_df, quarantine_df). A row can fail several rules at once."""
    reason = F.concat_ws("; ", *[F.when(cond, F.lit(text)) for cond, text in rules])
    # cache: the same tagged rows feed two outputs (silver + quarantine)
    tagged = df.withColumn("_reject_reason", reason).cache()
    valid = tagged.filter("_reject_reason = ''").drop("_reject_reason")
    bad = tagged.filter("_reject_reason != ''").withColumn("_quarantined_at", F.current_timestamp())
    return valid, bad


# ---------- table-by-table cleaning ----------

AUDIT = ["_ingested_at", "_source", "_batch_id"]


def clean_customers(raw):
    df = dedupe_latest(raw, "customer_id")
    valid, bad = split_valid(df, [(F.col("customer_id").isNull(), "missing customer_id")])
    valid = valid.select(
        "customer_id",
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        mask_email(F.col("email")).alias("email_hash"),          # PII masked
        mask_phone(F.col("phone")).alias("phone_masked"),        # PII masked
        standardise_city(F.col("city")).alias("city"),
        F.trim("state").alias("state"),
        F.col("signup_date").cast("date").alias("signup_date"),
        "updated_at", *AUDIT)
    return valid, bad


def clean_products(raw):
    df = dedupe_latest(raw, "product_id")
    valid, bad = split_valid(df, [(F.col("unit_price") < 0, "negative unit_price"),
                                  (F.col("product_id").isNull(), "missing product_id")])
    valid = valid.select("product_id", F.trim("product_name").alias("product_name"),
                         F.coalesce(F.col("category"), F.lit("Unknown")).alias("category"),  # default, not drop
                         "brand", F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
                         "updated_at", *AUDIT)
    return valid, bad


def clean_stores(raw):
    df = dedupe_latest(raw, "store_id")
    valid, bad = split_valid(df, [(F.col("store_id").isNull(), "missing store_id")])
    valid = valid.select("store_id", "store_name", standardise_city(F.col("city")).alias("city"),
                         F.upper(F.trim("region")).alias("region"), "opened_date", "updated_at", *AUDIT)
    return valid, bad


def clean_orders(raw, customers, stores):
    df = dedupe_latest(raw, "order_id").withColumn("order_ts", parse_legacy_date("order_date"))
    # Referential integrity: does the customer / store exist in clean silver?
    df = (df.join(customers.select(F.col("customer_id").alias("_c")), F.col("customer_id") == F.col("_c"), "left")
            .join(stores.select(F.col("store_id").alias("_s")), F.col("store_id") == F.col("_s"), "left"))
    valid, bad = split_valid(df, [
        (F.col("order_ts").isNull(), "unparseable order_date"),
        (F.col("order_ts") > F.current_timestamp(), "order_date in the future"),
        (F.col("order_ts") < F.lit("2015-01-01").cast("timestamp"), "order_date before 2015"),
        (F.col("customer_id").isNull(), "missing customer_id"),
        (F.col("customer_id").isNotNull() & F.col("_c").isNull(), "unknown customer_id"),
        (F.col("_s").isNull(), "unknown store_id"),
    ])
    valid = valid.select("order_id", "customer_id", "store_id", "order_ts",
                         F.to_date("order_ts").alias("order_date"),
                         F.upper("status").alias("order_status"), "payment_method", "updated_at", *AUDIT)
    return valid, bad.drop("_c", "_s")


def clean_order_items(raw, orders, products):
    df = dedupe_latest(raw, "order_item_id")
    df = (df.join(orders.select(F.col("order_id").alias("_o")), F.col("order_id") == F.col("_o"), "left")
            .join(products.select(F.col("product_id").alias("_p")), F.col("product_id") == F.col("_p"), "left"))
    valid, bad = split_valid(df, [
        (F.col("quantity") <= 0, "non-positive quantity"),
        (F.col("unit_price") < 0, "negative unit_price"),
        (F.col("_p").isNull(), "product missing or quarantined"),
        (F.col("_o").isNull(), "parent order missing or quarantined"),
    ])
    valid = valid.select("order_item_id", "order_id", "product_id", F.col("quantity").cast("int"),
                         F.col("unit_price").cast("decimal(10,2)"), F.col("discount_pct").cast("decimal(5,2)"),
                         "updated_at", *AUDIT)
    return valid, bad.drop("_o", "_p")


def write(spark, df, layer, table):
    path = lake_path(layer, table)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    if layer == "silver":
        spark.sql(f"CREATE TABLE IF NOT EXISTS silver.{table} USING DELTA LOCATION '{path}'")
    return spark.read.format("delta").load(path)


if __name__ == "__main__":
    spark = get_spark("silver")
    banner("Silver: dedupe + standardise + mask PII + quarantine")
    bronze = {t: spark.table(f"bronze.{t}") for t in ["customers", "products", "stores", "orders", "order_items"]}
    stats = {}

    def run(table, fn, *deps):
        t0 = time.time()
        raw = bronze[table]
        valid, bad = fn(raw, *deps)
        valid = write(spark, valid, "silver", table)
        bad = write(spark, bad, "quarantine", table)
        n_raw = raw.count()
        n_keys = raw.select(raw.columns[0]).distinct().count()
        s = {"bronze_rows": n_raw, "distinct_keys": n_keys,
             "duplicates_or_old_versions_removed": n_raw - n_keys,
             "silver_rows": valid.count(), "quarantined_rows": bad.count()}
        stats[table] = s
        print(f"  {table:<12} bronze={s['bronze_rows']:>7,}  -dups/old versions={s['duplicates_or_old_versions_removed']:>5,}"
              f"  -> silver={s['silver_rows']:>7,}  quarantine={s['quarantined_rows']:>4,}  ({time.time()-t0:.0f}s)")
        return valid

    customers = run("customers", clean_customers)
    products = run("products", clean_products)
    stores = run("stores", clean_stores)
    orders = run("orders", clean_orders, customers, stores)
    run("order_items", clean_order_items, orders, products)

    print("\n  Quarantine reasons:")
    for t in stats:
        q = spark.read.format("delta").load(lake_path("quarantine", t))
        for r in q.groupBy("_reject_reason").count().orderBy(F.desc("count")).collect():
            print(f"    {t:<12} {r['count']:>5}  {r['_reject_reason']}")
    (ROOT / "lake" / "_state").mkdir(parents=True, exist_ok=True)
    (ROOT / "lake" / "_state" / "silver_stats.json").write_text(json.dumps(stats, indent=2))
    spark.stop()
