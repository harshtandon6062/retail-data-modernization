"""Export the gold star schema as CSV for Power BI Desktop (exports/powerbi/).

Power BI can also connect live to Databricks/Synapse/BigQuery; CSV keeps the demo
portable. Governance: names/email hash are NOT exported - the BI model only gets
non-identifying customer attributes (same idea as gold.v_sales_analyst).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark  # noqa: E402

EXPORTS = {
    "fact_sales": "SELECT order_item_id, order_id, date_key, customer_sk, product_key, store_key, order_status, "
                  "payment_method, quantity, unit_price, discount_pct, gross_amount, discount_amount, net_amount "
                  "FROM gold.fact_sales",
    "dim_customer": "SELECT customer_sk, customer_id, city, state, signup_date, "
                    "CAST(effective_from AS DATE) AS effective_from, CAST(effective_to AS DATE) AS effective_to, "
                    "is_current FROM gold.dim_customer",
    "dim_product": "SELECT * FROM gold.dim_product",
    "dim_store": "SELECT * FROM gold.dim_store",
    "dim_date": "SELECT * FROM gold.dim_date",
}

if __name__ == "__main__":
    spark = get_spark("export_bi")
    banner("Export gold star schema for Power BI")
    out = ROOT / "exports" / "powerbi"
    out.mkdir(parents=True, exist_ok=True)
    for name, sql in EXPORTS.items():
        pdf = spark.sql(sql).toPandas()   # ~120k rows max: fine for pandas
        pdf.to_csv(out / f"{name}.csv", index=False)
        print(f"  exports/powerbi/{name}.csv  {len(pdf):>8,} rows")
    spark.stop()
