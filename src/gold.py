"""Gold layer = ELT. The data is already in the lake (Extract+Load done), so the
Transform is plain SQL executed by Spark: sql/gold/*.sql run in file-name order.

Why SQL files? Analysts and reviewers can read them without knowing Python,
and the same SQL ports to Databricks / Synapse / BigQuery with few changes.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark, lake_path  # noqa: E402

SQL_DIR = ROOT / "sql" / "gold"


def split_statements(sql_text):
    """Remove -- comments, then split on ';' (our SQL files never put ';' inside strings)."""
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def run_sql_file(spark, path, params=None):
    params = {"lake": lake_path(), **(params or {})}
    text = Path(path).read_text()
    for key, value in params.items():
        text = text.replace("${" + key + "}", value)
    for stmt in split_statements(text):
        spark.sql(stmt)


if __name__ == "__main__":
    spark = get_spark("gold")
    banner("Gold: star schema built with Spark SQL (ELT)")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    for f in sorted(SQL_DIR.glob("*.sql")):
        run_sql_file(spark, f)
        print(f"  ran {f.relative_to(ROOT)}")
    print()
    for t in ["dim_date", "dim_product", "dim_store", "dim_customer", "fact_sales"]:
        print(f"  gold.{t:<14} {spark.table('gold.' + t).count():>8,} rows")
    print("\n  SCD2 dim_customer: current vs historical versions")
    spark.sql("""SELECT is_current, count(*) AS versions, count(DISTINCT customer_id) AS customers
                 FROM gold.dim_customer GROUP BY is_current ORDER BY is_current DESC""").show()
    spark.stop()
