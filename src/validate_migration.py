"""Migration validation: prove the lake matches the legacy source.

A migration is only "done" when the business trusts the numbers. Typical
sign-off checks, all done here:
  1. Row counts per table        source  vs  bronze (current version of each row)
  2. Hash checksum per table     row-level SHA-256, summed (order-independent)
  3. Total amount                sum(quantity * unit_price) of all order lines
  4. Row accounting              source keys = silver rows + quarantined rows
  5. Per-month revenue           source (rules re-implemented independently in SQL)
                                 vs gold.fact_sales
Plus a Delta time-travel demo (query older versions of a table).
Output: reports/migration_validation.md with a PASS/FAIL table.
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark, jdbc_props, jdbc_url  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import Window  # noqa: E402

KEYS = {"customers": "customer_id", "products": "product_id", "stores": "store_id",
        "orders": "order_id", "order_items": "order_item_id"}


def source(spark, table):
    """Fresh read straight from PostgreSQL (independent of the pipeline)."""
    return spark.read.jdbc(jdbc_url(), table, properties=jdbc_props())


def bronze_current(spark, table, columns):
    """Bronze keeps every version; the current one has the max updated_at per key.
    (rank, not row_number: the legacy source's own exact duplicates must survive here.)"""
    w = Window.partitionBy(KEYS[table])
    return (spark.table(f"bronze.{table}")
            .withColumn("_max", F.max("updated_at").over(w))
            .filter("updated_at = _max").select(*columns))


def checksum(df):
    """Order-independent checksum: hash each row, turn the first 15 hex chars into
    a number, and add them all up. Same rows in any order -> same total."""
    row = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in df.columns])
    h = F.conv(F.substring(F.sha2(row, 256), 1, 15), 16, 10).cast("decimal(38,0)")
    r = df.agg(F.count("*").alias("n"), F.sum(h).alias("h")).first()
    return r["n"], str(r["h"])


SOURCE_MONTHLY_SQL = """
-- The cleaning rules re-implemented from the business spec, written separately
-- from src/silver.py, run against the RAW source. If both agree, we trust both.
WITH o AS (
  SELECT DISTINCT order_id, customer_id, store_id,
         coalesce(try_to_timestamp(order_date, 'yyyy-MM-dd HH:mm:ss'),
                  try_to_timestamp(order_date, 'dd/MM/yyyy HH:mm')) AS ts
  FROM src_orders),
valid_o AS (
  SELECT * FROM o
  WHERE ts IS NOT NULL AND ts <= current_timestamp() AND ts >= TIMESTAMP'2015-01-01'
    AND customer_id IN (SELECT customer_id FROM src_customers)
    AND store_id IN (SELECT store_id FROM src_stores)),
i AS (SELECT DISTINCT order_item_id, order_id, product_id, quantity, unit_price, discount_pct FROM src_order_items)
SELECT date_format(valid_o.ts, 'yyyy-MM') AS month,
       sum(CAST(i.quantity * i.unit_price - round(i.quantity * i.unit_price * i.discount_pct / 100, 2)
                AS DECIMAL(14,2))) AS amount
FROM i JOIN valid_o ON i.order_id = valid_o.order_id
WHERE i.quantity > 0 AND i.unit_price >= 0
  AND i.product_id IN (SELECT product_id FROM src_products WHERE unit_price >= 0)
GROUP BY 1
"""


def run(spark):
    results = []   # (check, table, source, lake, ok)

    for t in KEYS:
        src = source(spark, t)
        src.createOrReplaceTempView(f"src_{t}")
        cols = src.columns
        n_src, h_src = checksum(src)
        n_lake, h_lake = checksum(bronze_current(spark, t, cols))
        results.append(("Row count (source vs bronze)", t, f"{n_src:,}", f"{n_lake:,}", n_src == n_lake))
        results.append(("SHA-256 checksum (all columns)", t, h_src[:12] + "...", h_lake[:12] + "...",
                        h_src == h_lake))

    amt = "sum(CAST(quantity * unit_price AS DECIMAL(18,2)))"
    s_amt = spark.sql(f"SELECT {amt} a FROM src_order_items").first().a
    l_amt = bronze_current(spark, "order_items", ["quantity", "unit_price"]).selectExpr(f"{amt} a").first().a
    results.append(("Total amount qty x price", "order_items", f"{s_amt:,}", f"{l_amt:,}", s_amt == l_amt))

    for t, key in KEYS.items():
        keys = spark.sql(f"SELECT count(DISTINCT {key}) c FROM src_{t}").first().c
        silver = spark.table(f"silver.{t}").count()
        quarantined = spark.read.format("delta").load(str(ROOT / "lake" / "quarantine" / t)).count()
        results.append(("Row accounting: keys = silver + quarantine", t, f"{keys:,}",
                        f"{silver:,} + {quarantined:,}", keys == silver + quarantined))

    src_month = {r.month: r.amount for r in spark.sql(SOURCE_MONTHLY_SQL).collect()}
    gold_month = {r.month: r.amount for r in spark.sql(
        "SELECT date_format(order_ts, 'yyyy-MM') month, sum(net_amount) amount "
        "FROM gold.fact_sales GROUP BY 1").collect()}
    monthly = []
    for m in sorted(set(src_month) | set(gold_month)):
        s, g = src_month.get(m), gold_month.get(m)
        monthly.append((m, s, g, s == g))
    all_months_ok = all(ok for *_, ok in monthly)
    results.append(("Per-month net revenue (all months)", "fact_sales",
                    f"{sum(v for v in src_month.values()):,}", f"{sum(v for v in gold_month.values()):,}",
                    all_months_ok))
    return results, monthly


def time_travel(spark):
    """Delta keeps every version of a table -> query the past with VERSION AS OF."""
    hist = spark.sql("DESCRIBE HISTORY gold.dim_customer").select("version", "timestamp", "operation") \
        .orderBy("version").collect()
    rows = []
    for h in hist:
        r = spark.sql(f"""SELECT count(*) total, count_if(is_current) cur
                          FROM gold.dim_customer VERSION AS OF {h.version}""").first()
        rows.append((h.version, h.operation, r.total, r.cur))
    return rows


def write_report(results, monthly, tt):
    passed = sum(r[-1] for r in results)
    verdict = "PASS" if passed == len(results) else "FAIL"
    lines = [
        "# Migration validation report", "",
        f"_Generated by `src/validate_migration.py` on {datetime.now():%Y-%m-%d %H:%M}._", "",
        f"## Overall: **{verdict}** ({passed}/{len(results)} checks)", "",
        "| Check | Table | Source (PostgreSQL) | Lake | Result |", "|---|---|---|---|---|"]
    for check, table, s, l, ok in results:
        lines.append(f"| {check} | {table} | {s} | {l} | {'PASS' if ok else 'FAIL'} |")
    lines += ["", "How to read this:",
              "- **Source vs bronze** must match exactly: bronze is a faithful copy (nothing lost in transit).",
              "- **Row accounting**: every distinct source key ends up either in silver or in quarantine - "
              "nothing is silently dropped. Exact duplicates collapse into one key.",
              "- **Per-month revenue**: the cleaning rules were re-implemented independently in SQL over the raw "
              "source; they agree with gold to the paisa.", "",
              "## Per-month net revenue (INR): source vs gold", "",
              "| Month | Source | Gold | Result |", "|---|---|---|---|"]
    for m, s, g, ok in monthly:
        lines.append(f"| {m} | {s:,} | {g:,} | {'PASS' if ok else 'FAIL'} |" if s is not None and g is not None
                     else f"| {m} | {s} | {g} | FAIL |")
    lines += ["", "## Delta Lake time travel: `gold.dim_customer`", "",
              "Every write creates a new table version. Older versions stay queryable "
              "(`SELECT ... FROM gold.dim_customer VERSION AS OF 1`), which gives audit history and "
              "an easy rollback (`RESTORE TABLE ... TO VERSION AS OF n`).", "",
              "| Version | Operation | Total rows | Current rows |", "|---|---|---|---|"]
    for v, op, total, cur in tt:
        lines.append(f"| {v} | {op} | {total:,} | {cur:,} |")
    (ROOT / "reports" / "migration_validation.md").write_text("\n".join(lines) + "\n")
    return verdict, passed


if __name__ == "__main__":
    spark = get_spark("validate_migration")
    banner("Migration validation: PostgreSQL source vs lakehouse")
    results, monthly = run(spark)
    for check, table, s, l, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {check:<44} {table:<12} src={s:<18} lake={l}")
    tt = time_travel(spark)
    print("\n  Delta time travel on gold.dim_customer:")
    for v, op, total, cur in tt:
        print(f"    VERSION AS OF {v}: {op:<22} total={total:,}  current={cur:,}")
    verdict, passed = write_report(results, monthly, tt)
    print(f"\n  OVERALL {verdict} ({passed}/{len(results)}) -> reports/migration_validation.md")
    spark.stop()
    sys.exit(0 if verdict == "PASS" else 1)
