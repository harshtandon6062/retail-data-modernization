# How the project was built - step by step

For every step: **what I did**, **language & resources used (and why)**, **technical details** (for deep
questions), and **what it achieved compared with the legacy system ("before" -> "after")**.

Quick map:

| Step | What | Main language / tool |
|---|---|---|
| 0 | Environment & shared Spark setup | Python 3.11, Java 21, PySpark 4.0.1, Delta Lake 4.0.1, YAML |
| 1 | Build the legacy source system | Python + Faker + psycopg2, SQL (PostgreSQL 16) |
| 2 | Sqoop-style ingestion to bronze + CDC | PySpark (Spark JDBC), Delta Lake |
| 3 | Hive metastore + schema-on-read | HiveQL / Spark SQL, Apache Derby |
| 4 | Silver: clean, dedupe, mask PII, quarantine | PySpark DataFrame API |
| 5 | Gold: star schema + SCD2 | Spark SQL (Delta `MERGE`) |
| 6 | Incremental (CDC) round | Python + PySpark |
| 7 | Governance: quality, lineage, catalog | PySpark + Python (regex), Markdown/JSON/Mermaid |
| 8 | Migration validation + time travel | PySpark + Spark SQL, Delta |
| 9 | Analytics, Power BI export, dashboard | Spark SQL, pandas, Plotly, DAX (docs) |
| 10 | MapReduce-style job | PySpark RDD API |
| 11 | Automated tests | pytest |
| 12 | Automation | GNU Make, bash, log4j2 |
| 13 | Real-time streaming | Kafka (Docker), kafka-python, Spark Structured Streaming |
| 14 | Scala version | Scala 2.13 (spark-shell) |
| 15 | Cloud mapping + CI | YAML configs, GitHub Actions |
| 16 | Documentation | Markdown, Mermaid |

---

## Step 0 - Environment and the shared Spark setup

**What I did.** Set up a Python virtual environment with pinned package versions (`requirements.txt`), started
PostgreSQL and Docker, and wrote one helper file, `src/common.py`, that every pipeline step uses to create its
Spark session, read config, and build lake paths.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Python 3.11** | The language I know best; PySpark gives full Spark power from Python |
| **Java 21 (OpenJDK)** | Spark, Hive, Kafka and the JDBC driver all run on the JVM; Spark 4 supports Java 17/21 |
| **PySpark 4.0.1** | Distributed processing engine; same code works on a laptop or a 100-node cluster |
| **delta-spark 4.0.1** | Delta Lake = ACID transactions, MERGE, time travel on top of Parquet files; version must match Spark 4.0 |
| **Maven Central jars** (downloaded automatically via `spark.jars.packages`): `io.delta:delta-spark_2.13:4.0.1`, `org.postgresql:postgresql:42.7.7`, `spark-sql-kafka-0-10_2.13:4.0.1` | Java libraries Spark needs at runtime; `_2.13` = built for Scala 2.13, which Spark 4 uses |
| **PyYAML + `config/local.yaml`** | Keep settings (DB host, lake root, Kafka) out of code so changing environment = changing one file |
| **venv + pinned versions** | Reproducible: the same versions on my VM, a laptop and in CI |

**Technical details (`src/common.py` -> `get_spark`)**
- `master("local[*]")` - Spark runs inside one process using all CPU cores (on a cluster this would be `yarn` or Kubernetes).
- `spark.sql.extensions` + `spark.sql.catalog.spark_catalog` - switch on Delta Lake SQL (`MERGE`, `VERSION AS OF`).
- `spark.sql.shuffle.partitions = 8` - default is 200 (meant for big clusters); with small data 200 tiny tasks just add overhead.
- `spark.databricks.delta.snapshotPartitions = 2` - Delta uses 50 partitions to read its own transaction log by default; this made a 20-row write take ~4 s. Lowering it sped up silver.
- `spark.sql.session.timeZone = UTC` and `-Duser.timezone=UTC` - timestamps never shift between Postgres, Spark and Python (important for checksums).
- `enableHiveSupport()` + `spark.hadoop.javax.jdo.option.ConnectionURL` pointing to an embedded **Derby** database in `lake/_metastore` - a persistent Hive metastore.
- `lake_path()` - every path is built from `lake_root` in the config, so `lake/...` locally becomes `s3a://...` / `abfss://...` / `gs://...` in the cloud configs without code changes.
- `config/log4j2.properties` - keeps logs readable and silences two harmless Hive warnings.

**Before -> after.** Before: nothing - analytics ran ad hoc on the production database. After: one consistent,
portable processing platform, configured in one place, that can move to any cloud by changing a config file.

---

## Step 1 - Build the legacy source system (the "before")

**What I did.** `src/generate_legacy_db.py` creates a PostgreSQL database `legacy_retail` that behaves like a
15-year-old retail system, with realistic data and realistic problems. `--apply-changes` later simulates a day of
new business.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Python** | Easy to generate data with loops and randomness |
| **Faker (`en_IN` locale)** | Realistic Indian names; I built emails/phones myself to control formats |
| **psycopg2** | The standard Python driver for PostgreSQL |
| **SQL DDL** (`CREATE TABLE`) and **`COPY`** | `COPY` bulk-loads 120k rows in about a second (row-by-row `INSERT` would be much slower) |
| **PostgreSQL 16** | A real relational OLTP database, like the systems clients actually have |
| **Fixed random seed (42)** | Every run produces exactly the same data -> reproducible reports and tests |

**Technical details**
- 5 tables: `customers` (2,000 + 20 duplicates = 2,020), `products` (200), `stores` (20 across 15 cities, 4 regions),
  `orders` (50,000 + 400 duplicates = 50,400, Jan 2024 - Dec 2025), `order_items` (119,800). Each has `updated_at`.
- **No primary or foreign keys** on purpose (common in legacy systems) - that is how duplicates and orphans got in.
- `order_date` stored as **VARCHAR** (text), another classic legacy mistake.
- Business patterns: 2025 has ~15% more orders than 2024; October/November have +60% (Diwali/festive season).
- **Injected dirty data (~2%)**:
  - customers: 20 missing emails, 20 exact duplicate rows, ~4% city typos (`mumbai`, `MUMBAI `, ` Mumbai`), phones in 4 formats (`+91-98...`, `098...`, `98...`, `+91 98xxx xxxxx`)
  - products: 2 missing categories, 1 negative price (product 100)
  - orders: ~1% dates as `dd/MM/yyyy HH:mm`, ~0.3% garbage (`00/00/0000`, `2025-02-30 10:00:00`, `N/A`, empty), ~0.2% in year 2031, ~0.5% missing customer, 400 duplicates
  - order_items: ~0.8% negative quantities, ~0.2% product id 9999 that doesn't exist, 600 duplicates
- Supplier files in `lake/landing/suppliers/`: `master/supplier_master.csv` and two JSON Lines files; the March file has 3 extra fields (`transport` object, `batch_codes` array, `temperature_c`) = schema drift.
- `--apply-changes`: 25 customers move city, 10 prices +5%, 10 new customers, 200 new orders (2 lines each), 50 orders changed to RETURNED - all with `updated_at = now`.

**What this represents.** The *problems* the migration must solve: slow reporting on the live DB, duplicates, bad
dates, inconsistent text, exposed PII, no history, no documentation.

---

## Step 2 - Sqoop-style ingestion into bronze, with CDC

**What I did.** `src/ingest_jdbc.py` copies every table from PostgreSQL into the lake's **bronze** layer as Delta
tables, reading in parallel, adding audit columns, and remembering a **watermark** so the next run loads only changes.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **PySpark JDBC reader** | Does what **Sqoop** did (Sqoop was retired to the Apache Attic in 2021): parallel, split-by-column reads from an RDBMS |
| **PostgreSQL JDBC driver** (Java) | JDBC = the standard Java API to talk to databases; Spark is on the JVM so it uses JDBC |
| **Delta Lake** | Bronze must be appendable, reliable (ACID) and queryable - plain CSV/Parquet folders are not transactional |
| **JSON file** (`lake/_state/watermarks.json`) | Simple, visible place to store "how far did I load" per table |

**Technical details (`ingest_table`)**
1. Build a filter: first run = no filter (full load); later runs = `WHERE updated_at > '<watermark>'`. The filter is
   inside a subquery, so it runs **inside PostgreSQL** (predicate pushdown) - only changed rows cross the network.
2. Ask Postgres for `min(id), max(id), count(*)` of that set - Sqoop does the same to plan its splits.
3. Parallel read: `column=order_id, lowerBound=min, upperBound=max+1, numPartitions=4` -> Spark opens 4 connections,
   each running `... WHERE order_id >= x AND order_id < y`. (customers 2 readers, products/stores 1, orders/items 4.)
   `fetchsize=5000` = rows per network round-trip. Bounds only decide slice size - no rows are skipped.
4. Add audit columns: `_ingested_at`, `_source` (`postgres.legacy_retail.public.orders`), `_load_type` (full/incremental), `_batch_id`, `ingest_date`.
5. `append` to `lake/bronze/<table>` partitioned by `ingest_date` (one folder per day - easy to find or reprocess a day's load).
6. Save the new max `updated_at` as the watermark.
- Limitation I can explain: query-based CDC can't see hard DELETEs and depends on apps updating `updated_at`;
  log-based CDC (Debezium / AWS DMS / Azure Datastream reading the database log) fixes that.

**Result.** Full load: 172,440 rows. Incremental load after changes: 696 rows (36 customers, 10 products, 0 stores, 250 orders, 400 lines).

**Before -> after.**
- Before: every report queried the live OLTP database, competing with store billing. After: the source is read once, then only changes - **696 rows instead of 172,440**.
- Before: no record of when data arrived. After: every row carries load time, source and batch id (auditability).
- Before: if a report was wrong you couldn't see the original data. After: bronze is an exact, replayable copy - cleaning can be re-run any time without touching the source.

---

## Step 3 - Hive metastore and schema-on-read

**What I did.** `src/register_tables.py` registers all bronze folders as tables in a **Hive metastore** and handles the
semi-structured supplier files, including schema drift.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **HiveQL / Spark SQL** | Standard SQL interface to files in a lake |
| **Hive metastore on Apache Derby** | The metastore maps table name -> columns + file location. Derby is a small embedded Java DB - fine for one user locally; production uses a shared metastore (MySQL/Postgres) or Glue/Unity Catalog |
| **Spark JSON reader + Delta `mergeSchema`** | Read nested JSON and evolve the table schema safely |

**Technical details**
- `CREATE DATABASE bronze / silver / gold`.
- `CREATE TABLE bronze.orders USING DELTA LOCATION '<lake>/bronze/orders'` -> **external** tables (dropping the table never deletes data).
- A classic Hive table over the raw CSV folder - schema-on-read:
  `CREATE EXTERNAL TABLE bronze.supplier_master (...) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '...' TBLPROPERTIES ('skip.header.line.count'='1')`.
- Supplier JSON: January has 5 fields, March has 8. Writing January then appending March **fails** without
  `mergeSchema` (Delta schema enforcement, `AnalysisException`), and succeeds with `mergeSchema=true` - January rows get NULL for the new columns. Nested fields are queried as `transport.mode`; arrays with `size(batch_codes)`.
- If Hive support failed to start, the script falls back to Spark's in-memory catalog (not needed in practice).

**Before -> after.**
- Before: data only reachable through the OLTP app's database; supplier files handled manually and a new field breaks the load.
- After: every dataset is discoverable and queryable by name with SQL from any engine that reads a Hive metastore; schema changes from suppliers are absorbed in a controlled way.

---

## Step 4 - Silver: clean, deduplicate, standardise, mask PII, quarantine

**What I did.** `src/silver.py` turns bronze (every version of every row, dirt included) into **one clean, current,
privacy-safe row per key**, and moves rule-breaking rows to `lake/quarantine/` with a reason.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **PySpark DataFrame API** | Rules are easier to express, reuse and unit-test as small Python functions than as one huge SQL |
| **Spark built-in functions** (`sha2`, `regexp_replace`, `initcap`, `try_to_timestamp`, window functions) | Run inside the JVM - much faster than Python UDFs, and work at any scale |
| **Delta Lake** | Overwrite silver atomically; readers never see a half-written table |

**Technical details (functions in `src/silver.py`)**
- `dedupe_latest(df, key)`: window `PARTITION BY key ORDER BY updated_at DESC, _ingested_at DESC`, keep `row_number() = 1`.
  Removes both CDC history (older versions) and exact duplicates. ROW_NUMBER (not RANK) guarantees exactly one row.
- `parse_legacy_date`: `coalesce(try_to_timestamp(x, 'yyyy-MM-dd HH:mm:ss'), try_to_timestamp(x, 'dd/MM/yyyy HH:mm'))`.
  Spark 4 runs in **ANSI mode**, where `to_timestamp('2025-02-30')` throws an error and kills the job; `try_` returns NULL instead, and the row is quarantined.
- `standardise_city`: `initcap(lower(trim(city)))` -> `Mumbai`.
- `mask_email`: `sha2(lower(trim(email)), 256)` - same email always gives the same 64-character hash, so you can still count/join customers.
- `mask_phone`: strip non-digits, keep last 10, show `XXXXXX` + last 4.
- Missing product category -> `'Unknown'` (a default, not a rejection).
- `split_valid(df, rules)`: each rule is (condition, reason). `concat_ws('; ', when(cond, reason)...)` builds a combined reason; empty = valid. Valid rows -> silver, others -> quarantine with `_reject_reason` + `_quarantined_at`. The tagged DataFrame is **cached** because it feeds two writes.
- Referential checks via left joins: order's customer/store must exist in clean silver; an order line's order and product must exist (so a quarantined order also quarantines its lines - "parent order missing or quarantined").
- Rules: unparseable date, future date, date before 2015, missing/unknown customer, unknown store, quantity <= 0, negative price, unknown product.
- Stats saved to `lake/_state/silver_stats.json` for reports.

**Result (after the CDC round).** Customers 2,010; orders 49,705 + 495 quarantined; order lines 116,560 + 3,040 quarantined; 1 product quarantined. Total 3,536 quarantined rows, every one with a reason.

**Before -> after.**
- Before: 400 duplicate orders and 600 duplicate lines inflated revenue. After: exactly one row per order/line.
- Before: dates in two formats + impossible dates broke monthly reports. After: one proper timestamp type; bad dates isolated.
- Before: `Mumbai` counted as 3 different cities. After: one standard spelling.
- Before: raw emails/phones visible to anyone querying. After: hashed/masked in every layer analysts use.
- Before: bad data silently mixed with good data. After: bad data is separated, explained and traceable (a data steward can fix it at the source).

---

## Step 5 - Gold: star schema with SCD Type 2 (ELT in SQL)

**What I did.** `src/gold.py` runs the SQL files in `sql/gold/` in order to build a **star schema** for BI, plus a
PII-free analyst view.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Spark SQL in `.sql` files** | ELT: data is already loaded, transformations are plain SQL that analysts/reviewers can read; the same SQL ports to Databricks/Synapse/BigQuery |
| **Delta `MERGE`** | Lets us update + insert in one atomic statement - essential for SCD2 |
| **Small Python runner** (`run_sql_file`) | Strips `--` comments, splits on `;`, substitutes `${lake}` with the lake path, executes each statement |

**Technical details**
- `01_dim_date.sql`: `explode(sequence(first day, last day, INTERVAL 1 DAY))` - one row per day; `date_key` = `yyyymmdd` integer; year, quarter, month, weekday, weekend flag, **Indian financial year** (April-March).
- `02_dim_product.sql`, `03_dim_store.sql`: **Type 1** (overwrite) dimensions; `price_band` Budget/Mid/Premium.
- `04_dim_customer_scd2.sql` (**Type 2**):
  1. Create the table if missing (columns incl. `customer_sk`, `row_hash`, `effective_from`, `effective_to`, `is_current`).
  2. `customer_changes`: silver customers that are **new** or whose hash of tracked attributes (`sha2(city|state)`) differs from the current row.
  3. Merge source = changes twice: once with `merge_key = customer_id` (matches current row -> **UPDATE** `is_current=false, effective_to=updated_at`), once with `merge_key = NULL` for existing customers (never matches -> **INSERT** new version).
  4. New surrogate keys = `max(customer_sk) + row_number()`.
  5. First version valid from 1900-01-01 (so old orders find it); later versions from the change time; open versions end 9999-12-31.
  - Idempotent: running it again with the same data changes nothing.
- `05_fact_sales.sql`: **grain = one order line**. Keys: `date_key`, `customer_sk`, `product_key`, `store_key`; `order_id` as degenerate dimension; measures `quantity`, `gross_amount`, `discount_amount`, `net_amount` as `DECIMAL(14,2)` (exact money, no floating-point errors). Customer join is **point-in-time**: `order_ts >= effective_from AND order_ts < effective_to`.
- `06_v_sales_analyst.sql`: joins fact + dims, exposes no names/email/phone; GRANT statements documented.

**Result.** fact_sales 116,560 rows; dim_customer 2,033 rows (2,010 current + 23 history); dim_date 1,035 days.

**Before -> after.**
- Before: reports joined 5 normalised OLTP tables on a busy server. After: one fact + four dimensions - simple, fast queries; Power BI-ready.
- Before: when a customer moved, the old city was overwritten - history lost. After: every version kept; each sale is attributed to the city the customer lived in **at the time of the order**.
- Before: revenue logic scattered in different reports. After: one definition (`net_amount`) in one place.
- Before: everyone saw personal data. After: analysts use a view without it (role-based access).

---

## Step 6 - The CDC round (keeping the lake in sync)

**What I did.** `make cdc`: ran `generate_legacy_db.py --apply-changes`, then ingestion, silver and gold again.

**Language & resources.** Same as steps 1-2-4-5 (Python, PySpark, Spark SQL, Delta).

**Technical details.** The watermark made ingestion read only rows with a newer `updated_at` and **append** them to
bronze with `_load_type = incremental`. Silver's `dedupe_latest` picks the newest version automatically. The SCD2
MERGE closed 23 old customer versions and inserted 23 new ones + 10 new customers. Two of the 25 "moved" customers
were given the same city they already had - correctly **no** new version, because the hash didn't change.

**Before -> after.** Before: getting fresh data meant a full re-export. After: near-continuous sync with tiny loads -
this is exactly how a real migration keeps the new platform current until cutover day.

---

## Step 7 - Governance: data quality, lineage, catalog

**What I did.** `src/quality_checks.py` and `src/lineage.py`.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Spark SQL** count queries | Each check is one aggregate query - scales to any size |
| **Python `re` (regex)** | Parse the gold SQL files to discover which tables feed which (automatic lineage) |
| **Markdown, JSON, Mermaid** | Human-readable reports, machine-readable lineage, diagrams that GitHub renders |
| **Delta `DESCRIBE HISTORY`** | Adds the current table version to each lineage node |

**Technical details**
- 21 checks in 4 dimensions: **completeness** (% non-null, e.g. email_hash 99.00% - the 20 customers with no email are kept and reported, not deleted), **uniqueness** (keys, one current SCD2 row per customer), **validity** (quantity > 0, net between 0 and gross, dates in range, email is a 64-char hex hash, phone masked format, region in allowed list, SCD2 window valid), **referential integrity** (fact keys exist in each dimension: 0 orphans).
- Lineage = hand-declared Python edges (source -> bronze -> silver -> quarantine -> exports) + edges parsed from `FROM/JOIN/USING` and `CREATE TABLE/VIEW` in the SQL; each node gets row count and Delta version -> `reports/lineage.json` and a Mermaid diagram.
- Catalog: reads the real schemas from the metastore and writes every column's type, description, **PII flag** and **owner** -> `docs/data_catalog.md`.

**Before -> after.** Before: nobody could prove data quality, explain where a number came from, or say which columns are
personal data. After: 21/21 checks pass with a report, lineage from source column to report, and a catalog with PII
flags and owners - what auditors and the DPDP Act expect.

---

## Step 8 - Migration validation and time travel

**What I did.** `src/validate_migration.py` re-reads the **source directly from PostgreSQL** and reconciles it with the
lake, then shows Delta time travel. Writes `reports/migration_validation.md`; exits with an error code if anything fails.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Spark JDBC** (fresh read, independent of the pipeline) | Compare against the true source, not against our own copy |
| **Spark SQL** (`SOURCE_MONTHLY_SQL`) | Re-implements the business rules independently - if two separate implementations agree, both are probably right |
| **SHA-256 via Spark `sha2`** | Detects any changed value, not just missing rows |
| **Delta time travel** | Prove history/rollback works |

**Technical details**
- Bronze "current" view: for each key, rows whose `updated_at` = max per key (uses `max() over window`, so the source's own exact duplicates remain - otherwise counts wouldn't match the source).
- Checksum: per row, concatenate all columns as text -> SHA-256 -> take the first 15 hex characters -> convert to a number -> **sum** over the table. Order of rows doesn't matter; any single changed value changes the sum.
- Checks (17): row count and checksum for each of 5 tables; total `quantity x unit_price`; **row accounting** (distinct source keys = silver rows + quarantined rows, per table); per-month net revenue, source vs gold.
- Time travel: `DESCRIBE HISTORY gold.dim_customer` and `SELECT ... VERSION AS OF n` -> v0 empty, v1 2,000 rows, v2 2,033 rows.

**Result.** OVERALL PASS 17/17; total amount ₹1,089,641,141.17 on both sides; net revenue ₹1,029,432,964.78 on both sides.

**Before -> after.** Before: "Is the new system right?" had no answer - so the business would never switch off the old
reports. After: documented evidence that every row and every rupee is accounted for - the sign-off needed for cutover.
Plus the ability to look at or restore any earlier version of a table (audit, rollback).

---

## Step 9 - Analytics, Power BI export and dashboard

**What I did.** `src/analytics.py` (5 business queries), `src/export_bi.py` (CSV for Power BI), `src/dashboard.py`
(HTML dashboard), `docs/POWERBI.md` (rebuild in Power BI with relationships and DAX).

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Spark SQL window functions** (`LAG`, `RANK`, `first_value`) | Growth, ranking and cohort retention without self-joins |
| **pandas** | Results are small aggregates - convenient to save as CSV and compute KPIs |
| **Plotly** (Python) | Interactive charts in one self-contained HTML file (works offline, no server) |
| **Power BI + DAX** (documented) | The BI tool named in the JD; DAX for measures like YoY % |

**Technical details**
- Revenue definition: `net_amount` for orders not CANCELLED/RETURNED. Queries read the **PII-free view**.
- Q1 monthly trend + MoM growth (`LAG`); Q2 top-3 products per region (`RANK` - ties share a rank); Q3 retention by first-purchase quarter; Q4 store YoY growth (`LAG`, only complete years); Q5 average items per order and AOV by region/year.
- Export excludes names and email hash (governance applies to BI too).
- Dashboard: 5 KPI cards + 4 charts; plotly.js embedded (~4.7 MB) so it opens offline; colour-blind-safe palette.

**Result.** Net revenue ₹92.44 Cr, 44,387 orders, 2,010 customers, AOV ₹20,825, YoY +14.2%; Diwali peak visible (Oct 2024 ₹5.42 Cr vs ~₹3.2 Cr normal month); top store growth Chandigarh #7 +27.6%.

**Before -> after.** Before: slow, inconsistent reports straight off the OLTP database, no YoY or cohort insight. After:
consistent KPIs from one trusted model, interactive dashboard, Power BI-ready model.

---

## Step 10 - MapReduce-style job (Hadoop concept)

**What I did.** `src/mapreduce_sales_by_region.py`: net sales per region with the low-level **RDD** API.

**Language & resources.** PySpark RDD API (`textFile`, `map`, `reduceByKey`, `broadcast`). Why: to show the Hadoop
MapReduce idea (map -> shuffle -> reduce) and how Spark runs it in memory.

**Technical details.** Read CSV lines -> `map` each line to `(region, net_amount)` (region looked up from a
**broadcast** dictionary, sent once to each worker) -> `reduceByKey(+)` (pre-sums inside each partition before the
shuffle, like a Hadoop "combiner") -> sort. Prints the RDD lineage (Spark's DAG).
Result: NORTH ₹27.86 Cr, WEST ₹27.27 Cr, EAST ₹18.97 Cr, SOUTH ₹18.33 Cr.

---

## Step 11 - Automated tests

**What I did.** `tests/test_silver.py` and `tests/test_scd2.py` (6 tests).

**Language & resources.** **pytest** + a local Spark session (`tests/conftest.py`, in-memory catalog so tests never
touch the pipeline's metastore). Why: prove the critical logic works and keeps working after changes.

**What they check.** Dedupe keeps the newest version and drops exact duplicates; email hash is consistent, 64 chars,
NULL stays NULL; all 4 phone formats -> `XXXXXX3210`; both date formats parse and `2025-02-30`/`N/A` become NULL;
quarantine keeps every row (valid + bad = input); SCD2 - a city change creates a new version, closes the old one at the
change time, leaves unchanged customers alone, keeps surrogate keys unique, and a re-run changes nothing.
The SCD2 test runs the **real** SQL file.

---

## Step 12 - Automation (Makefile)

**What I did.** One `Makefile`: `make all` runs everything in order, `make clean` deletes all generated output; also
`make stream`, `make scala`, `make kafka-up/down`.

**Language & resources.** GNU Make + bash (`pipefail` so a failing step stops the run; a filter that hides Maven
download chatter). Why: one command to reproduce everything - the same idea as an orchestrator (Airflow / Azure Data
Factory) in production. Full run from scratch: exit 0 in 9 min 07 s (most of that is starting Spark 13 times).

---

## Step 13 - Real-time streaming with Kafka

**What I did.** `docker-compose.yml` (Kafka), `src/stream_producer.py`, `src/stream_consumer.py`.

**Language & resources - and why**
| Resource | Why |
|---|---|
| **Docker + `apache/kafka:3.9.0`** in **KRaft** mode | Real Kafka in one container, no ZooKeeper needed |
| **kafka-python** | Simple Python producer |
| **Spark Structured Streaming** + `spark-sql-kafka` connector | Same DataFrame API for streams; exactly-once into Delta |

**Technical details**
- Topic `retail.events`, **3 partitions**.
- Producer: ~40 events/second for 50 s (page_view, add_to_cart, order_placed); **key = customer_id** (same customer -> same partition -> order kept per customer); `acks=all` (broker confirms storage); ~5% events deliberately 1-15 minutes late.
- Consumer: reads from `earliest` offset; parses JSON with an **explicit schema**; writes raw events to `lake/bronze/clickstream` every 10 s; aggregates by **5-minute window** and event type with a **10-minute watermark** (late events within 10 minutes still counted, later ones dropped and window state freed) into `lake/bronze/clickstream_5min`; **checkpoints** store processed offsets so a restart resumes without loss or duplicates.
- Result: 2,000 events in = 2,000 rows out; offsets per partition visible. (A second run without the checkpoint re-read the retained topic -> 4,000 rows: shows Kafka retention/replay and why checkpoints matter.)

**Before -> after.** Before: only batch data from the OLTP database, no view of website/app behaviour. After: events
land in the lake within seconds, with live 5-minute summaries (e.g. orders and revenue per window).

---

## Step 14 - Scala version

**What I did.** `scala/GoldRevenue.scala`, run by `make scala`.

**Language & resources.** **Scala 2.13** in the `spark-shell` that ships with PySpark, plus the Delta jar. Why: Spark is
written in Scala; the JD lists Scala. Same DataFrame logic (filter, join to dim_date, groupBy, sum) -> **identical
numbers** to the SQL version (e.g. 2024-01 ₹32,869,699.63; Year 2025 ₹490,225,612.43).
Detail: `spark-shell -i file` is ignored by Spark 4's Scala 2.13 shell when not run interactively, so the Makefile
pipes `:paste scala/GoldRevenue.scala` into the shell.

---

## Step 15 - Cloud mapping and CI

**What I did.** `config/aws.yaml`, `azure.yaml`, `gcp.yaml` (only storage paths and endpoints change:
`s3a://`, `abfss://`, `gs://`), `docs/CLOUD_MAPPING.md` (service-by-service table + Azure architecture diagram),
`docs/HADOOP_MAPPING.md`, and `.github/workflows/ci.yml`.

**Language & resources.** YAML; **GitHub Actions** with a PostgreSQL 16 service container, Java 21 (Temurin), Python
3.11, a cache for downloaded jars, and the reports uploaded as an artifact. Why: proves on a clean machine, on every
push, that tests and the whole batch pipeline work (it ran and passed after merge).

**Before -> after.** Before: tied to one on-prem server, no automated checks. After: cloud-portable design
(not deployed) and automatic verification on every change.

---

## Step 16 - Documentation

`README.md` (story, architecture, real results, JD skill map), `docs/LEARN.md` (concepts), `docs/INTERVIEW.md`
(pitch, before/after, tough questions, glossary), `docs/POWERBI.md`, this file. Written in Markdown with Mermaid
diagrams so GitHub renders them.

---

## The whole thing in one paragraph (for "summarise your project")

"I simulated a messy legacy PostgreSQL retail database in Python, copied it in parallel with Spark JDBC into a Delta
Lake bronze layer with watermark-based CDC, registered everything in a Hive metastore, cleaned and PII-masked it in
silver with a quarantine for bad rows, modelled it as a star schema with an SCD2 customer dimension using Spark SQL and
Delta MERGE, governed it with 21 quality checks, lineage and a catalog, proved the migration with 17 reconciliation
checks including SHA-256 checksums, and served it through analytics SQL, a Plotly dashboard and a Power BI model -
plus Kafka streaming, a Scala job, a MapReduce demo, tests, a Makefile and CI."
