"""SCD Type 2 test: runs the REAL sql/gold/04_dim_customer_scd2.sql against tiny tables."""
from datetime import datetime
from pathlib import Path

from gold import run_sql_file

SQL = Path(__file__).resolve().parent.parent / "sql" / "gold" / "04_dim_customer_scd2.sql"
SCHEMA = ("customer_id int, first_name string, last_name string, email_hash string, phone_masked string, "
          "city string, state string, signup_date date, updated_at timestamp")


def load_silver(spark, rows, path):
    spark.createDataFrame(rows, SCHEMA).write.format("delta").mode("overwrite").save(path)


def test_scd2_creates_new_version_on_city_change(spark, tmp_path):
    lake = str(tmp_path)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{lake}/silver_db'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION '{lake}/gold_db'")
    silver_path = f"{lake}/silver/customers"
    d0, d1 = datetime(2024, 1, 1), datetime(2025, 6, 1)

    # Day 1: two customers
    load_silver(spark, [(1, "Asha", "Rao", "h1", "XXXXXX0001", "Pune", "Maharashtra", None, d0),
                        (2, "Ravi", "Iyer", "h2", "XXXXXX0002", "Chennai", "Tamil Nadu", None, d0)], silver_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customers USING DELTA LOCATION '{silver_path}'")
    run_sql_file(spark, SQL, {"lake": lake})
    assert spark.table("gold.dim_customer").count() == 2

    # Day 2: customer 1 moves to Bengaluru, customer 3 is new, customer 2 unchanged
    load_silver(spark, [(1, "Asha", "Rao", "h1", "XXXXXX0001", "Bengaluru", "Karnataka", None, d1),
                        (2, "Ravi", "Iyer", "h2", "XXXXXX0002", "Chennai", "Tamil Nadu", None, d0),
                        (3, "Meera", "Das", "h3", "XXXXXX0003", "Kolkata", "West Bengal", None, d1)], silver_path)
    run_sql_file(spark, SQL, {"lake": lake})
    dim = spark.table("gold.dim_customer")
    rows = {(r.customer_id, r.city): r for r in dim.collect()}

    assert dim.count() == 4                                         # 2 + new version + new customer
    old, new = rows[(1, "Pune")], rows[(1, "Bengaluru")]
    assert old.is_current is False and old.effective_to == d1       # old version closed at change time
    assert new.is_current is True and new.effective_from == d1      # new version opens at change time
    assert rows[(2, "Chennai")].is_current is True                  # unchanged customer untouched
    assert dim.select("customer_sk").distinct().count() == 4        # surrogate keys unique
    assert dim.filter("is_current").groupBy("customer_id").count().filter("count > 1").count() == 0

    # Day 3: same data again -> nothing changes (the load is idempotent)
    run_sql_file(spark, SQL, {"lake": lake})
    assert spark.table("gold.dim_customer").count() == 4
