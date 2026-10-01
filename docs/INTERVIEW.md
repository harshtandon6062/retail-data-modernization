# Interview kit

Contents: [Why I built this](#why-i-built-this-what-to-say) ·
[Before vs after the migration](#what-the-migration-achieves-before-vs-after) ·
[Step-by-step: what the project does](#step-by-step-what-exactly-happens) ·
[60-second pitch](#60-second-pitch) · [3-minute walkthrough](#3-minute-walkthrough-open-these-files-in-order) ·
[Challenges](#challenges-i-hit-while-building-it-all-real) · [Production changes](#what-id-change-in-production) ·
[Scope notes](#honest-scope-notes-say-these-before-they-ask) · [Full forms (glossary)](#full-forms-of-every-abbreviation-used) · Detailed build steps: [BUILD_STEPS.md](BUILD_STEPS.md)

## Why I built this (what to say)

When the interviewer asks *"Why did you build this project?"* - say it in your own words, along these lines:

> "I read the job description for the Application & Data Modernization & Migration role and saw that the work is
> about taking companies off old, on-premises systems and moving their data to modern platforms - data lakes,
> warehouses and the cloud - with governance and BI on top. I knew Python, SQL, MySQL and Pandas from college,
> but I had never seen how those pieces fit together in a real migration. So instead of only reading about
> Hadoop, Hive, Spark and Kafka, I built one complete, small migration end to end: an old, messy database ->
> a modern lakehouse -> trusted reports. The aim was to understand the *why* behind each tool, not just
> its name.
>
> I chose retail because it's easy to relate to - customers, products, stores, orders - and I made the data
> Indian (cities, regions, the Diwali sales spike, rupees, the DPDP Act) so the business questions feel real.
> I deliberately put dirty data in the source, because in a real migration the hard part isn't copying data,
> it's proving that nothing was lost or corrupted and that the business can trust the new numbers. That's why
> the project ends with a validation report where all 17 checks pass, including checksums and month-by-month
> revenue matching the old system to the paisa."

Short follow-ups you can use:
- *"What did you learn?"* - "That migration is mostly about data quality, reconciliation and governance.
  The tools change every few years; the discipline of 'prove the numbers match' doesn't."
- *"Why not just copy the tables?"* - "A plain copy moves the problems too: duplicates, wrong dates, exposed
  personal data, and no history. The point of modernisation is that the new platform is *better*, not just newer."
- *"Is it a real company?"* - "No, RetailCo is fictional and the data is synthetic but realistic - generated with
  a fixed seed so every run gives the same numbers, which is also good practice for testing."

## What the migration achieves (before vs after)

**The "before" picture (the legacy system):** RetailCo runs everything on one on-premises PostgreSQL database
that the billing/store app writes to. Managers' reports run directly on that same database.

| Problem in the legacy system | Why it hurts the business | What we have after the migration | Where in the repo |
|---|---|---|---|
| Reports run on the live OLTP database | Heavy reporting queries slow down billing at the stores; reports are slow | Analytics runs on a separate lakehouse; the OLTP database is only *read* once per load, and incremental loads read only changed rows (696 instead of ~172,000) | `src/ingest_jdbc.py` |
| No history: when a customer moves city, the old city is overwritten | You can't answer "how much did customers *who lived in Pune at the time* buy?" | SCD Type 2 customer dimension keeps every version with valid-from/valid-to dates; each sale points to the version valid on the order date | `sql/gold/04_dim_customer_scd2.sql` |
| No primary/foreign keys -> duplicate orders, orders for non-existent products/customers | Revenue is double-counted; totals differ between reports | Duplicates removed (400 orders, 600 lines); every fact row has a valid date, customer, product and store (referential integrity checks 0 orphans) | `src/silver.py`, `reports/data_quality.md` |
| Dates stored as text in 2 formats, plus impossible dates (`2025-02-30`, `N/A`, year 2031) | Monthly reports are wrong or crash | Both formats parsed into a real timestamp; impossible dates are quarantined | `src/silver.py` `parse_legacy_date` |
| Bad rows (negative quantities, missing customer) silently mixed in | Nobody knows the data is wrong | Bad rows go to a quarantine area *with a reason*, so they can be fixed at the source - nothing is silently lost (3,536 rows quarantined in total, each explained) | `lake/quarantine/`, `reports/data_quality.md` |
| Same city typed as `mumbai`, `MUMBAI `, ` Mumbai` | Region/city reports split one city into 3 rows | Standardised to `Mumbai` | `src/silver.py` `standardise_city` |
| Raw email and phone visible to anyone who can query | Privacy risk; non-compliance with India's DPDP Act 2023 | Email hashed (SHA-256), phone masked to last 4 digits; analysts get a view without names | `src/silver.py`, `sql/gold/06_v_sales_analyst.sql` |
| No documentation of tables or columns | New joiners and auditors can't tell what a column means or whether it's personal data | Data catalog with description, PII flag and owner for every column; lineage diagram showing where each table comes from | `docs/data_catalog.md`, `reports/lineage.md` |
| Supplier files (CSV/JSON) handled by hand; when the supplier adds fields, loading breaks | Manual work, broken loads | Read with schema-on-read; new fields merged automatically (`mergeSchema`) | `src/register_tables.py` |
| Data only in tables built for transactions, not analysis | Every report needs complex joins on a busy DB | A star schema (1 fact + 4 dimensions) designed for BI; Power BI / dashboard can use it directly | `sql/gold/`, `docs/POWERBI.md` |
| No way to see what the data looked like last week | Can't audit or roll back a bad load | Delta Lake time travel: query any earlier version (`VERSION AS OF`) | `src/validate_migration.py` |
| Only batch data, once a day at best | No live view of website/app activity | Kafka + Spark Streaming bring clickstream events into the lake every 10 seconds, with 5-minute summaries | `src/stream_consumer.py` |
| Everything tied to one on-prem server | Hard to scale for Diwali peaks; hardware cost | Storage and compute separated; the same code is mapped to AWS/Azure/GCP by changing one config value | `config/*.yaml`, `docs/CLOUD_MAPPING.md` |
| "Is the new system right?" - no proof | Business won't switch off the old reports | Migration validation report: 17/17 PASS (row counts, SHA-256 checksums, totals, month-by-month revenue) | `reports/migration_validation.md` |

**One-line summary for the interviewer:** "Before, RetailCo had one slow, messy, history-less database that exposed
personal data. After, it has a governed lakehouse with clean, deduplicated, privacy-masked, historical data in a
star schema, near-real-time events, a catalog and lineage, and a report proving the numbers match the old system."

## Step-by-step: what exactly happens

Run order = the order of `make all`. Each step says **what it does**, **why**, and **what you get** (real numbers).

**Step 1 - Create the legacy source system** (`src/generate_legacy_db.py`)
- *What:* Creates a PostgreSQL database `legacy_retail` with 5 tables: customers (2,020 rows), products (200),
  stores (20, across 15 Indian cities in 4 regions), orders (50,400 over Jan 2024 - Dec 2025) and order_items (119,800).
  Every table has an `updated_at` column. About 2% of rows are deliberately bad. Also drops supplier files
  (1 CSV + 2 JSON) into `lake/landing/suppliers/`.
- *Why:* To simulate a real 15-year-old system - no constraints, text dates, typos - so the migration has real problems to solve.
- *You get:* A realistic "before" system to migrate from.

**Step 2 - Ingest into Bronze, the Sqoop way** (`src/ingest_jdbc.py`)
- *What:* Spark connects to PostgreSQL over JDBC. For each table it first asks for the min/max ID, then opens
  several connections in parallel, each reading one slice of IDs (e.g. orders in 4 slices). Rows are written
  **unchanged** into Delta tables in `lake/bronze/`, plus audit columns: when loaded (`_ingested_at`), from where
  (`_source`), which batch (`_batch_id`), full or incremental (`_load_type`). It saves the highest `updated_at`
  per table in `lake/_state/watermarks.json`.
- *Why:* Bronze is an exact, replayable copy. If cleaning has a bug later, we rebuild from bronze without touching the old DB again.
- *You get:* 172,440 rows copied; the watermark for the next run.

**Step 3 - Register tables in the Hive metastore + read supplier files** (`src/register_tables.py`)
- *What:* Creates databases `bronze`, `silver`, `gold` in a Hive metastore and registers each bronze folder as an
  **external table** (the metastore stores only name, columns and location). Creates a classic Hive table over the
  raw supplier CSV. Reads the two supplier JSON files - the March file has 3 new fields - and merges their
  schemas (`mergeSchema`); without it, the load fails (shown in the output).
- *Why:* So anyone can query the lake with SQL by table name; and to show how to handle schema drift.
- *You get:* 7 bronze tables queryable with SQL.

**Step 4 - Clean into Silver** (`src/silver.py`)
- *What:* For each table: (1) keep only the latest version of each row by ID (removes duplicates and old CDC
  versions); (2) fix formats - trim/standardise names and cities, parse both date formats, upper-case regions;
  (3) mask personal data - email -> SHA-256 hash, phone -> `XXXXXX1234`; (4) check rules - missing customer,
  impossible/future date, quantity <= 0, negative price, unknown product/customer/store - and send failing rows to
  `lake/quarantine/<table>` with a `_reject_reason`.
- *Why:* Gold and reports must only see clean, trustworthy, privacy-safe data - but nothing may disappear without a trace.
- *You get:* 49,705 clean orders + 495 quarantined; 116,560 clean lines + 3,040 quarantined (after the CDC round).

**Step 5 - Build the Gold star schema** (`src/gold.py` runs `sql/gold/01..06_*.sql`)
- *What:* Plain Spark SQL builds `dim_date` (one row per day, incl. Indian financial year), `dim_product`,
  `dim_store`, `dim_customer` (SCD Type 2 via a Delta `MERGE`) and `fact_sales` (one row per order line, with
  gross, discount and net amount). Also creates `v_sales_analyst`, a view without personal data.
- *Why:* A star schema is the standard shape for BI: simple joins, fast aggregations, easy for Power BI.
- *You get:* fact_sales 116,560 rows; dim_customer 2,000 versions on first load.

**Step 6 - Simulate new business activity and load only the changes (CDC)** (`make cdc`)
- *What:* `generate_legacy_db.py --apply-changes` makes 25 customers move city, changes 10 prices, adds 10
  customers and 200 orders, and marks 50 orders as returned - all with a new `updated_at`. Then
  `ingest_jdbc.py` runs again and, because a watermark exists, reads **only rows with `updated_at` after the
  watermark**. Silver and gold are rebuilt; the SCD2 MERGE closes old customer versions and adds new ones.
- *Why:* A real migration does one big load, then keeps the new platform in sync until cutover day.
- *You get:* Only 36 customers, 10 products, 250 orders, 400 lines read (instead of ~172k). dim_customer now has
  2,033 rows: 2,010 current + 23 history rows (2 of the 25 "moves" were to the same city, so correctly no new version).

**Step 7 - Governance** (`src/quality_checks.py`, `src/lineage.py`)
- *What:* 21 data-quality checks in 4 dimensions (completeness, uniqueness, validity, referential integrity) ->
  `reports/data_quality.md`. Lineage from the pipeline definition + by parsing the SQL files -> `reports/lineage.json`
  and a diagram. A data catalog generated from the live tables -> `docs/data_catalog.md`.
- *Why:* Trust, discoverability and compliance (who owns it, is it personal data, where did it come from).
- *You get:* 21/21 PASS, a lineage graph and a catalog.

**Step 8 - Migration validation** (`src/validate_migration.py`)
- *What:* Reads the source again directly from PostgreSQL and compares with the lake: row counts per table; a
  SHA-256 checksum of every row (summed, so order doesn't matter); total amount; "source keys = silver +
  quarantine"; and revenue per month, where the cleaning rules are re-written independently in SQL against the raw
  source. Also shows Delta time travel (versions 0, 1, 2 of `dim_customer`).
- *Why:* This is the sign-off evidence a client needs before switching off the old reports.
- *You get:* `reports/migration_validation.md` - OVERALL PASS, 17/17; e.g. revenue ₹1,029,432,964.78 on both sides.

**Step 9 - Analytics, BI exports and dashboard** (`src/analytics.py`, `src/export_bi.py`, `src/dashboard.py`)
- *What:* 5 business SQL queries (monthly trend with LAG, top-3 products per region with RANK, retention
  cohorts, store year-on-year growth, average basket). Exports the star schema as CSV for Power BI (without personal
  data). Builds `dashboard/index.html` with 5 KPI cards and 4 charts.
- *Why:* The business value - decisions are made from these numbers.
- *You get:* Net revenue ₹92.44 Cr, 44,387 orders, AOV ₹20,825, YoY +14.2%, Diwali peak visible in Oct/Nov.

**Step 10 - MapReduce demo and tests** (`src/mapreduce_sales_by_region.py`, `tests/`)
- *What:* Sales per region using map + reduceByKey (the Hadoop MapReduce idea on Spark). 6 automated tests
  check dedupe, masking, date parsing, quarantine and SCD2.
- *You get:* NORTH ₹27.86 Cr is the top region; 6/6 tests pass.

**Extra steps (separate commands)**
- `make stream`: starts Kafka in Docker; a producer sends ~40 website/app events per second; Spark Structured
  Streaming reads them every 10 s, parses JSON, groups them into 5-minute windows (waiting up to 10 minutes for late
  events) and writes to `lake/bronze/clickstream`, saving its progress (offsets) in a checkpoint.
- `make scala`: the monthly revenue calculation written in Scala - same numbers as the SQL version.

## Tough question: "Raw PII is still in bronze - so what's the point of hashing it in silver?"

**Short answer to say:**
> "Good question - masking in silver alone does *not* protect the data; it reduces who can see it. The protection
> of bronze comes from access control, encryption and retention. Different layers have different audiences:
> bronze is touched only by the ingestion pipeline and 2-3 platform engineers, while silver and gold are read by
> hundreds of analysts, dashboards and exports. Hashing in silver means personal data never reaches that wide
> audience. In my local project bronze isn't locked down because Spark local mode has no users - in production
> I'd protect it like this..."

**How bronze is protected in production (defence in depth):**
1. **Least-privilege access** - bronze lives in its own storage container / schema; only the ingestion service
   account and a small engineering group have read access (Unity Catalog grants, AWS IAM / Lake Formation, Azure RBAC).
   Analysts get **no** grant on bronze - only on `gold.v_sales_analyst`.
2. **Encryption** - at rest with keys in Key Vault / KMS (customer-managed keys), and in transit (TLS/SSL on the
   JDBC connection and storage).
3. **Network isolation** - private endpoints, no public access to the storage account.
4. **Audit logging** - every read of bronze is logged and reviewed (who read what, when).
5. **Retention / storage limitation** (a DPDP Act principle) - raw data is kept only as long as needed for
   reprocessing (e.g. 30-90 days), then deleted. With Delta you must also run `VACUUM`, otherwise old file
   versions kept for time travel still contain the PII.
6. **Right to erasure** - when a customer asks to be deleted: `DELETE` the rows in every layer + `VACUUM`; or
   "crypto-shredding" (encrypt each customer's PII with their own key and delete the key).

**Even better - don't let raw PII land at all (shift-left):**
- **Data minimisation**: if no report needs the email, don't extract it from the source (`SELECT` only needed columns).
- **Mask/encrypt at ingestion**: hash or encrypt PII columns inside the ingestion job *before* writing bronze,
  or replace them with **tokens** whose mapping lives in a separate secure vault (reversible only by authorised people,
  e.g. for customer service).
- **Trade-off**: bronze is then no longer an exact copy of the source, so migration checksums are computed on the
  masked values, and you can't re-derive anything from raw email later. Many teams accept that trade-off.

**One more improvement I'd make:** a plain SHA-256 of an email can be reversed by guessing (hash a list of known
emails and compare). In production I'd use a **keyed/salted hash (HMAC-SHA-256)** with the secret key in Key Vault,
so the hash still works for joins and counting but can't be brute-forced.

**Why hash and not just drop the email?** Because analysts still need to count unique customers and join
across systems (e.g. match the same customer in the CRM) - the hash keeps that ability without revealing the email.

## 60-second pitch

> "I built a legacy-to-lakehouse migration for a fictional Indian retail chain, RetailCo. The legacy system is a
> PostgreSQL OLTP database with about 50,000 orders and 120,000 order lines over two years, and I deliberately
> injected around 2% dirty data - duplicates, NULLs, negative quantities, dates stored as text in two formats.
> I ingest it Sqoop-style with parallel Spark JDBC reads into a Delta Lake bronze layer, with incremental CDC
> loads driven by an `updated_at` watermark. Silver cleans and deduplicates the data, masks PII - SHA-256 for
> emails, last-four-digits for phones - and sends invalid rows to a quarantine with a reason instead of dropping
> them. Gold is a star schema written in Spark SQL, with an SCD Type 2 customer dimension built with a Delta MERGE.
> On top I have data-quality checks, lineage, a data catalog with PII flags, and a migration validation report
> that reconciles source and lake with row counts, SHA-256 checksums and per-month revenue - 17 of 17 checks
> pass. There's also a Kafka plus Spark Structured Streaming pipeline, a Scala version of the revenue job, a
> Plotly dashboard and a Power BI guide. It all runs locally with one `make all`; the cloud part is mapped to
> AWS, Azure and GCP but not deployed."

## 3-minute walkthrough (open these files in order)

1. **The problem (20 s)** - `README.md` diagram. "Reporting runs on the OLTP database; it's slow, has no
   history and the data is messy. Goal: move analytics to a governed lakehouse without losing a single row."
2. **Legacy source (20 s)** - `src/generate_legacy_db.py`: no PK/FK constraints, `order_date` as VARCHAR,
   seeded so results are reproducible. Show the `DDL` and `build_orders_and_items`.
3. **Ingestion = Sqoop replacement (30 s)** - `src/ingest_jdbc.py` `ingest_table`: min/max query, then
   `partitionColumn/lowerBound/upperBound/numPartitions`; audit columns; watermark file. "After
   `--apply-changes` the incremental run read only 36 customers and 250 orders."
4. **Silver (30 s)** - `src/silver.py`: `dedupe_latest` (window + row_number), `parse_legacy_date`
   (try_to_timestamp with two formats), `mask_email`/`mask_phone`, `split_valid` -> quarantine with reasons.
   "495 orders and 3,040 lines quarantined, each with a reason."
5. **Gold (30 s)** - `sql/gold/04_dim_customer_scd2.sql`: the MERGE with the NULL-merge-key trick;
   `05_fact_sales.sql`: grain = order line, point-in-time join to the SCD2 dimension.
6. **Trust (30 s)** - `reports/migration_validation.md`: 17/17 PASS incl. checksums and per-month revenue
   computed independently; `reports/data_quality.md` 21/21; `docs/data_catalog.md`; `sql/gold/06_v_sales_analyst.sql`.
7. **Consumption (20 s)** - `dashboard/index.html`, `docs/POWERBI.md` (relationships + DAX).
8. **Extras (20 s)** - Kafka streaming with watermark/windows (`src/stream_consumer.py`), Scala job,
   MapReduce-style RDD job, `docs/CLOUD_MAPPING.md` (Azure target architecture).

## Challenges I hit while building it (all real)

| # | Problem | Cause | Fix / lesson |
|---|---|---|---|
| 1 | The Hive metastore was created in the working directory, not in `lake/_metastore`; Spark printed `Ignoring non-Spark config property: javax.jdo.option.ConnectionURL` | Spark only forwards Hadoop/Hive settings that start with `spark.hadoop.` | Renamed to `spark.hadoop.javax.jdo.option.ConnectionURL` (`src/common.py`). Lesson: read warnings, not just errors. |
| 2 | Silver took ~83 s for tiny tables; even a 20-row Delta write took ~4 s | Delta rebuilds table state with 50 shuffle partitions by default (tuned for clusters); and the same cleaned DataFrame was computed twice (silver + quarantine writes) | Set `spark.databricks.delta.snapshotPartitions=2`, `shuffle.partitions=8`, and `.cache()` the tagged DataFrame in `split_valid`. Lesson: defaults are for big clusters. |
| 3 | `ERROR HiveAlterHandler ... columns have types incompatible` on every `CREATE OR REPLACE TABLE` in gold, but the tables were fine | Hive tries to store the Delta schema in a Hive-compatible way, fails, then Spark falls back to its own format | Verified the data, then silenced only those two loggers in `config/log4j2.properties`. Lesson: distinguish noisy logs from real failures - check the result. |
| 4 | Legacy `order_date` values like `2025-02-30 10:00:00`, `00/00/0000`, `N/A`, and a second format `dd/MM/yyyy HH:mm` | Dates stored as text in the legacy app | `parse_legacy_date`: `coalesce(try_to_timestamp(fmt1), try_to_timestamp(fmt2))`. Spark 4 runs in ANSI mode, so plain `to_timestamp` would *fail the job* on bad values; `try_` returns NULL and the row is quarantined. |
| 5 | ~600 valid-looking order lines were quarantined as "unknown product" | One product had a negative price and was quarantined, so its order lines lost their parent | Renamed the reason to "product missing or quarantined" so the cascade is explicit. In production: fix the product at source and replay. Lesson: quarantine rules cascade through foreign keys. |
| 6 | Migration row counts would not have matched with a normal dedupe | The source itself contains exact duplicate rows; bronze holds every CDC version | For the source-vs-bronze check I take the *current version* per key with `rank()` (keeps the source's own duplicates), and separately prove `distinct keys = silver + quarantine`. |
| 7 | 25 customers "moved city" but SCD2 only created 23 new versions | 2 customers were randomly re-assigned the city they already had | Not a bug: SCD2 compares a hash of tracked attributes and only versions real changes. Good example of idempotency. |
| 8 | Kafka container exited at start: `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0` | Kafka 3.9's KRaft config validation | Use `KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093` (empty host = all interfaces) in `docker-compose.yml`. |
| 9 | Second streaming run wrote 4,000 events although the producer sent 2,000 | I had deleted the checkpoint; Kafka still retained the first run's events and the consumer starts from `earliest` | This is exactly why checkpoints exist: they store offsets so a restart resumes instead of replaying. Also shows Kafka retention/replay. |
| 10 | `spark-shell -i scala/GoldRevenue.scala` started the shell but never ran the script | Spark 4's Scala 2.13 REPL ignores `-i` when not attached to a terminal | Feed `:paste scala/GoldRevenue.scala` on stdin (`make scala`). Then a typed `Dataset[YearTotal]` failed with `EXPRESSION_DECODING_FAILED` because REPL classes are inner classes; I used `Row.getAs` instead. |
| 11 | `make` output hid all Spark result tables | My log filter removed lines starting with `|` (meant for Ivy's download report) | Narrowed the filter to tab-indented Ivy lines only. Lesson: verify what your log filters hide. |
| 12 | The first dashboard chart was cut off on the right | Plotly drew charts before the CSS grid had computed panel widths | Resize every chart on page load (`Plotly.Plots.resize`) and `min-width:0` on grid items. |

## What I'd change in production

- **Orchestration**: Airflow / Azure Data Factory / Databricks Workflows instead of `make`, with retries,
  SLAs and alerts.
- **CDC**: log-based CDC (Debezium, AWS DMS, Datastream) to capture DELETEs and avoid timestamp gaps; or
  keep the watermark but read with an overlap window and dedupe on the key.
- **Incremental silver/gold**: `MERGE` only changed keys instead of rebuilding silver from all of bronze; partition
  `fact_sales` by month; Z-order/cluster by common filters; `OPTIMIZE` + `VACUUM` with a retention policy.
- **Metastore & governance**: Unity Catalog / Glue Catalog / Purview instead of embedded Derby; real `GRANT`s,
  column masking and row filters; automatic lineage (OpenLineage); PII classification scans.
- **Security / DPDP**: secrets in Key Vault / Secrets Manager (not YAML), encryption at rest and in transit, raw
  bronze restricted and retention-limited, salted hashing or tokenisation (a plain SHA-256 of an email can be
  brute-forced if someone has a list of emails), consent and erasure workflow.
- **Quality**: Great Expectations / Deequ / dbt tests that *block* promotion to gold on failure; quarantine
  dashboards and a replay process with the data owners.
- **Testing & CI/CD**: more unit tests, integration tests on sample data, infra as code (Terraform), separate
  dev/test/prod workspaces.
- **Scale**: real cluster sizing, adaptive query execution, broadcast joins for dims, avoid `toPandas()` exports
  (BI connects directly to the gold layer).

## Honest scope notes (say these before they ask)

- Runs **locally** on one VM: PostgreSQL 16, Spark 4.0.1 in `local[*]` mode, Delta Lake 4.0.1, Kafka in Docker.
- **Cloud is mapped, not deployed**: `config/aws|azure|gcp.yaml` and `docs/CLOUD_MAPPING.md` show the design;
  nothing ran on AWS/Azure/GCP.
- **Hadoop is mapped, not installed**: no HDFS/YARN cluster; `docs/HADOOP_MAPPING.md` explains the mapping and the
  MapReduce-style job runs on Spark's RDD API.
- **Sqoop** itself is not used (it's retired); Spark JDBC does the same job, explained in `src/ingest_jdbc.py`.
- **Power BI**: I wrote the build guide and DAX and exported the model as CSV; the `.pbix` file is not in the repo.
  Tableau/Qlik are not used. The live dashboard is a Plotly HTML page.
- **Data is synthetic** (Faker, seeded): numbers are realistic in shape (festive peaks, ~14% growth) but not real.
  Retention looks very high because synthetic customers buy uniformly.
- **RBAC** is illustrated with a PII-free view and documented GRANTs; Spark local mode has no users to enforce them.
- **C/C++** is not part of this project.


## Full forms of every abbreviation used

**Data and architecture**
| Short form | Full form | One-line meaning |
|---|---|---|
| OLTP | Online Transaction Processing | Systems that record day-to-day transactions (billing, orders) - many small writes |
| OLAP | Online Analytical Processing | Systems built for analysis/reports - big reads and aggregations |
| ETL | Extract, Transform, Load | Clean the data *before* loading it into the target |
| ELT | Extract, Load, Transform | Load raw data first, transform inside the platform (used here) |
| DW / EDW | Data Warehouse / Enterprise Data Warehouse | Central store of clean, structured data for reporting |
| ACID | Atomicity, Consistency, Isolation, Durability | Guarantees that a write is all-or-nothing, valid, isolated from readers, and permanent |
| CDC | Change Data Capture | Loading only rows that changed since the last load |
| SCD | Slowly Changing Dimension | Technique to handle changes in dimension data; Type 1 = overwrite, Type 2 = keep history |
| PK / FK | Primary Key / Foreign Key | Unique row identifier / a column pointing to another table's primary key |
| SK | Surrogate Key | An artificial key made by the warehouse (here `customer_sk`), not from the source |
| DQ | Data Quality | Completeness, uniqueness, validity, consistency, referential integrity |
| RI | Referential Integrity | Every foreign key value exists in the parent table |
| PII | Personally Identifiable Information | Data that identifies a person: name, email, phone |
| DPDP Act | Digital Personal Data Protection Act, 2023 | India's data-privacy law |
| RBAC | Role-Based Access Control | Permissions given to roles (analyst, engineer), not to individuals |
| 6 Rs | Rehost, Replatform, Refactor/Re-architect, Repurchase, Retire, Retain | The standard cloud migration strategies |
| BI | Business Intelligence | Turning data into reports and dashboards |
| KPI | Key Performance Indicator | A headline business number (revenue, AOV) |

**SQL, programming and file formats**
| Short form | Full form | One-line meaning |
|---|---|---|
| SQL | Structured Query Language | Language for querying databases |
| DDL / DML | Data Definition Language / Data Manipulation Language | `CREATE/DROP` vs `INSERT/UPDATE/DELETE/MERGE` |
| CTE | Common Table Expression | The `WITH name AS (...)` blocks used in the analytics SQL |
| HiveQL | Hive Query Language | Hive's SQL dialect |
| JDBC | Java Database Connectivity | Standard Java API to connect to databases; Spark uses it to read PostgreSQL |
| API | Application Programming Interface | A defined way for programs to talk to each other |
| JVM | Java Virtual Machine | Runs Java/Scala code; Spark, Kafka and Hive run on it |
| JDK | Java Development Kit | Java compiler and runtime (Java 21 here) |
| RDD | Resilient Distributed Dataset | Spark's low-level distributed collection (used in the MapReduce demo) |
| DAG | Directed Acyclic Graph | The plan of steps Spark builds before running a job |
| UDF | User-Defined Function | Custom function in Spark; Python UDFs are slow, so built-ins were used |
| CSV | Comma-Separated Values | Plain-text table file |
| JSON | JavaScript Object Notation | Text format for nested data (supplier files, Kafka events) |
| YAML | YAML Ain't Markup Language | Human-readable config format (`config/*.yaml`) |
| HTML | HyperText Markup Language | The dashboard page format |
| SHA-256 | Secure Hash Algorithm, 256-bit | One-way hash used to mask emails and to checksum rows |
| ANSI (mode) | American National Standards Institute | Spark 4's strict SQL mode: invalid casts raise errors instead of returning NULL |
| UTC | Coordinated Universal Time | Time zone used so timestamps never shift |
| DAX | Data Analysis Expressions | Power BI's formula language for measures |
| VARCHAR | Variable Character | Text column type (the legacy `order_date` was stored as this) |

**Big data and streaming tools**
| Short form / name | Full form | One-line meaning |
|---|---|---|
| HDFS | Hadoop Distributed File System | Hadoop's storage: files split into 128 MB blocks, 3 copies each |
| YARN | Yet Another Resource Negotiator | Hadoop's cluster resource manager |
| MapReduce | - | Hadoop's compute model: map -> shuffle -> reduce |
| Sqoop | "SQL-to-Hadoop" | Retired tool for bulk-copying databases into Hadoop |
| Hive | - | SQL on top of files + the metastore (table catalogue) |
| KRaft | Kafka Raft (consensus protocol) | Kafka mode without ZooKeeper |
| Delta Lake | - | Open table format adding ACID, MERGE and time travel to Parquet files |
| Parquet | - | Column-oriented file format used under Delta |
| Derby | Apache Derby | Small embedded Java database used to store the Hive metastore locally |

**Cloud**
| Short form | Full form |
|---|---|
| AWS | Amazon Web Services |
| GCP | Google Cloud Platform |
| S3 / s3a:// | Simple Storage Service / Hadoop's "S3A" connector URL scheme |
| ADLS / abfss:// | Azure Data Lake Storage (Gen2) / Azure Blob File System (Secure) URL scheme |
| GCS / gs:// | Google Cloud Storage / its URL scheme |
| EMR | Elastic MapReduce (AWS managed Spark/Hadoop) |
| RDS | Relational Database Service (AWS) |
| DMS | Database Migration Service (AWS) |
| MSK | Managed Streaming for Apache Kafka (AWS) |
| ADF | Azure Data Factory |
| IR | Integration Runtime (ADF's agent that reaches on-prem databases) |
| IAM | Identity and Access Management |
| SaaS | Software as a Service |
| VM | Virtual Machine |
| IaC | Infrastructure as Code (e.g. Terraform) |
| SLA | Service Level Agreement |

**Business, India-specific and project process**
| Short form | Full form |
|---|---|
| AOV | Average Order Value (revenue / number of orders) |
| YoY / MoM | Year over Year / Month over Month (growth %) |
| FY | Financial Year (India: 1 April - 31 March) |
| INR / ₹ | Indian Rupee |
| Cr | Crore = 1,00,00,000 = 10 million |
| GSTIN | Goods and Services Tax Identification Number (in the supplier file) |
| UPI | Unified Payments Interface (a payment method in the data) |
| POS | Point of Sale (store billing system) |
| CI / CD | Continuous Integration / Continuous Delivery (GitHub Actions runs tests + pipeline on every push) |
| HA | High Availability |
| ML | Machine Learning |
| JD | Job Description |
