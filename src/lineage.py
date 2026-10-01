"""Lineage + data catalog.

LINEAGE answers "where did this number come from?". We build it from two sources:
  1. the pipeline definition (which script reads what and writes what), and
  2. the gold SQL files themselves (parsed for FROM/JOIN -> CREATE targets),
then enrich every node with runtime facts from Delta (row count, table version).
Outputs: reports/lineage.json (machine-readable) and reports/lineage.md (Mermaid).
In production: OpenLineage/Marquez, Microsoft Purview, Unity Catalog or Dataplex
capture this automatically.

CATALOG answers "what is this column and may I use it?" -> docs/data_catalog.md
(table, column, type, description, PII flag, owner).
"""
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROOT, banner, get_spark  # noqa: E402

ENTITIES = ["customers", "products", "stores", "orders", "order_items"]

# Edges that live in Python code (source -> target, via which script)
PIPELINE_EDGES = (
    [(f"postgres.{t}", f"bronze.{t}", "src/ingest_jdbc.py (JDBC, CDC)") for t in ENTITIES]
    + [("landing.suppliers_json", "bronze.supplier_deliveries", "src/register_tables.py (mergeSchema)"),
       ("landing.suppliers_csv", "bronze.supplier_master", "src/register_tables.py (Hive external table)")]
    + [(f"bronze.{t}", f"silver.{t}", "src/silver.py") for t in ENTITIES]
    + [(f"bronze.{t}", f"quarantine.{t}", "src/silver.py (rejected rows)") for t in ENTITIES]
    + [("silver.customers", "silver.orders", "src/silver.py (FK check)"),
       ("silver.stores", "silver.orders", "src/silver.py (FK check)"),
       ("silver.orders", "silver.order_items", "src/silver.py (FK check)"),
       ("silver.products", "silver.order_items", "src/silver.py (FK check)")]
    + [(f"gold.{t}", f"exports.powerbi.{t}", "src/export_bi.py")
       for t in ["fact_sales", "dim_customer", "dim_product", "dim_store", "dim_date"]]
)

LAYER_OWNER = {"bronze": "Data Engineering (ingestion)", "silver": "Data Engineering (platform)",
               "gold": "Analytics Engineering / BI"}

DESCRIPTIONS = {
    "customer_id": "Legacy business key of the customer", "customer_sk": "Surrogate key, one per SCD2 version",
    "first_name": "Customer first name", "last_name": "Customer last name",
    "email_hash": "SHA-256 of lower-cased email (pseudonymised)", "phone_masked": "Phone with only last 4 digits",
    "city": "City (standardised Title Case)", "state": "Indian state", "signup_date": "Date the customer registered",
    "row_hash": "Hash of SCD2-tracked attributes (city, state)", "effective_from": "SCD2 version valid from (inclusive)",
    "effective_to": "SCD2 version valid to (exclusive); 9999-12-31 = open", "is_current": "True for the latest version",
    "product_id": "Legacy product key", "product_key": "Product key (durable legacy id)", "product_name": "Product name",
    "category": "Product category ('Unknown' if missing at source)", "brand": "Brand", "unit_price": "Price per unit (INR)",
    "current_unit_price": "Current list price (INR)", "price_band": "Budget / Mid / Premium",
    "store_id": "Legacy store key", "store_key": "Store key (durable legacy id)", "store_name": "Store name",
    "region": "Sales region (NORTH/SOUTH/EAST/WEST)", "opened_date": "Store opening date",
    "order_id": "Order number (degenerate dimension in the fact)", "order_ts": "Order timestamp (parsed from legacy text)",
    "order_date": "Order calendar date", "order_status": "DELIVERED / SHIPPED / CANCELLED / RETURNED",
    "payment_method": "UPI / CARD / CASH / WALLET", "order_item_id": "Order line id - grain of fact_sales",
    "quantity": "Units sold on the line", "discount_pct": "Line discount %", "gross_amount": "quantity x unit_price (INR)",
    "discount_amount": "Discount value (INR)", "net_amount": "gross - discount (INR) - main revenue measure",
    "date_key": "Date key yyyymmdd", "full_date": "Calendar date", "year": "Calendar year", "quarter": "Calendar quarter",
    "month": "Month number", "month_name": "Month name", "year_month": "yyyy-MM", "day_of_week": "1=Sunday ... 7=Saturday",
    "day_name": "Weekday name", "is_weekend": "Saturday or Sunday", "fiscal_year": "Indian FY (Apr-Mar), e.g. FY2025",
    "updated_at": "Last change time in the legacy system (CDC column)", "_ingested_at": "When the row landed in bronze",
    "_source": "Source system.table", "_batch_id": "Ingestion batch id",
}
# PII = personal data under India's DPDP Act 2023 (direct or indirect identifiers)
PII = {"first_name": "Yes", "last_name": "Yes", "email_hash": "Pseudonymised", "phone_masked": "Masked",
       "email": "Yes", "phone": "Yes"}


def sql_edges():
    edges = []
    for f in sorted((ROOT / "sql" / "gold").glob("*.sql")):
        text = re.sub(r"--[^\n]*", "", f.read_text())
        targets = re.findall(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(gold\.\w+)",
                             text, re.I)
        sources = set(re.findall(r"(?:FROM|JOIN|USING)\s+((?:silver|gold)\.\w+)", text, re.I))
        for tgt in set(targets):
            edges += [(src, tgt, f"sql/gold/{f.name}") for src in sorted(sources) if src != tgt]
    return edges


def table_facts(spark, name):
    """Row count + Delta version for a lake table (None for external systems)."""
    layer = name.split(".")[0]
    if layer not in ("bronze", "silver", "gold", "quarantine"):
        return {}
    try:
        df = (spark.read.format("delta").load(str(ROOT / "lake" / "quarantine" / name.split(".")[1]))
              if layer == "quarantine" else spark.table(name))
        facts = {"rows": df.count()}
        if layer != "quarantine" and not name.startswith("gold.v_"):
            facts["delta_version"] = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"]
        return facts
    except Exception:
        return {"note": "not a Delta table"}


def write_catalog(spark):
    lines = ["# Data catalog", "",
             f"_Generated by `src/lineage.py` from the live metastore on {datetime.now():%Y-%m-%d %H:%M}._", "",
             "PII flag: **Yes** = direct identifier, **Masked/Pseudonymised** = transformed so it no longer "
             "identifies a person on its own (still treated as personal data under the DPDP Act).", ""]
    for db in ("silver", "gold"):
        for t in sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {db}").collect()):
            lines += [f"## `{db}.{t}`", "", f"Owner: {LAYER_OWNER[db]}", "",
                      "| Column | Type | Description | PII |", "|---|---|---|---|"]
            for field in spark.table(f"{db}.{t}").schema.fields:
                lines.append(f"| {field.name} | {field.dataType.simpleString()} | "
                             f"{DESCRIPTIONS.get(field.name, '')} | {PII.get(field.name, 'No')} |")
            lines.append("")
    (ROOT / "docs" / "data_catalog.md").write_text("\n".join(lines))


def mermaid(edges):
    def nid(n):
        return n.replace(".", "_")
    out = ["flowchart LR"]
    for src, tgt, _ in edges:
        out.append(f"    {nid(src)}[{src}] --> {nid(tgt)}[{tgt}]")
    return "\n".join(out)


if __name__ == "__main__":
    spark = get_spark("lineage")
    banner("Lineage + data catalog")
    edges = PIPELINE_EDGES + sql_edges()
    nodes = sorted({n for e in edges for n in e[:2]})
    lineage = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "nodes": [{"name": n, "layer": n.split(".")[0], **table_facts(spark, n)} for n in nodes],
        "edges": [{"from": s, "to": t, "via": v} for s, t, v in edges],
    }
    (ROOT / "reports" / "lineage.json").write_text(json.dumps(lineage, indent=2))
    main_path = [e for e in edges if "quarantine" not in e[1] and "FK check" not in e[2]
                 and "exports" not in e[1]]
    (ROOT / "reports" / "lineage.md").write_text(
        "# Lineage\n\n_Generated by `src/lineage.py`. Full graph with row counts and Delta versions: "
        "`reports/lineage.json`._\n\n```mermaid\n" + mermaid(main_path) + "\n```\n")
    write_catalog(spark)
    print(f"  {len(nodes)} nodes, {len(edges)} edges -> reports/lineage.json, reports/lineage.md")
    print("  catalog -> docs/data_catalog.md")
    print("  Example: what feeds gold.fact_sales?")
    for s, t, v in edges:
        if t == "gold.fact_sales":
            print(f"    {s:<22} via {v}")
    spark.stop()
