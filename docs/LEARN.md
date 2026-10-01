# 60-minute study guide

How to use this: read one section, open the file it points to, read the code for 1-2 minutes, then
say the two interview answers out loud. Sections are ordered from **most to least likely to be asked**
for an *Analyst - Application & Data Modernization & Migration* role. Times are suggestions.

| # | Topic | Minutes |
|---|---|---|
| 1 | ETL vs ELT | 3 |
| 2 | Data warehouse vs data lake vs lakehouse | 4 |
| 3 | Star schema, facts/dimensions, SCD Type 2 | 6 |
| 4 | Medallion architecture (bronze/silver/gold) | 3 |
| 5 | Data migration: 6 Rs, validation, cutover | 6 |
| 6 | Data governance: quality, lineage, catalog, PII, RBAC, DPDP Act | 6 |
| 7 | Spark: lazy evaluation, DAG, partitions, transformations vs actions | 5 |
| 8 | Cloud service mapping (AWS / Azure / GCP) | 3 |
| 9 | Schema-on-read vs schema-on-write | 2 |
| 10 | Hadoop: HDFS, YARN, MapReduce | 4 |
| 11 | Hive and the metastore | 3 |
| 12 | Kafka | 4 |
| 13 | Sqoop and CDC | 3 |
| 14 | Delta Lake: ACID and time travel | 3 |
| 15 | BI: Power BI, DAX, SQL window functions | 3 |
| 16 | Scala | 2 |

---

## 1. ETL vs ELT

**What it is.** ETL = Extract, Transform, Load: data is cleaned on a separate server *before* it is loaded
into the warehouse. ELT = Extract, Load, Transform: raw data is loaded first into cheap scalable storage,
then transformed *inside* the platform (usually with SQL). Cloud and lakehouse platforms mostly use ELT
because storage is cheap and compute scales.

**Analogy.** ETL = washing and cutting vegetables at the farm before shipping them. ELT = shipping everything
to a big kitchen and preparing it there - you keep the raw vegetables in case you need a different recipe later.

**In this repo.** Extract+Load = `src/ingest_jdbc.py` (`ingest_table`) puts raw rows into bronze unchanged.
Transform = `src/silver.py` (PySpark) and the SQL files in `sql/gold/` run by `src/gold.py` (`run_sql_file`).
So this project is **ELT**.

**Interview Q&A**
- *Q: Why did you choose ELT?* A: Raw data lands in bronze first, so if a business rule changes (say, how we
  treat returns) I can re-run the transform from bronze without going back to the legacy system. Spark does
  the transforms at scale, and the gold layer is plain SQL that analysts can read.
- *Q: When is ETL still better?* A: When sensitive data must be masked *before* it lands anywhere (strict
  compliance), or when the target is an expensive warehouse where you don't want to store raw data.
  (Here I kept raw PII only in bronze, which would be locked down, and masked it in silver.)

## 2. Data warehouse vs data lake vs lakehouse

**What it is.** A **data warehouse** stores structured, cleaned data in tables with a fixed schema; great
for BI, but costly and rigid. A **data lake** stores any file (CSV, JSON, Parquet, images) cheaply in object
storage; flexible but can become a "data swamp" without transactions or quality. A **lakehouse** = lake
storage + warehouse features (ACID transactions, schemas, MERGE, time travel) using an open table format such
as **Delta Lake**, Iceberg or Hudi.

**Analogy.** Warehouse = a supermarket: everything labelled on the right shelf. Lake = a big storeroom:
everything fits, but good luck finding it. Lakehouse = the storeroom with a barcode system and a logbook.

**In this repo.** All tables are Delta tables in `lake/` (lake storage, Parquet files) with warehouse-style
star schema in `lake/gold/` and SQL access through the metastore. See `src/common.py` (`get_spark`, the Delta
config lines).

**Interview Q&A**
- *Q: What makes your project a lakehouse and not just a lake?* A: The files are Parquet on cheap storage, but
  Delta adds a transaction log, so I get ACID writes, `MERGE` for SCD2, schema enforcement plus controlled
  schema evolution (`mergeSchema`), and time travel - all things a plain folder of files can't do.
- *Q: Name formats/products.* A: Open table formats Delta Lake, Apache Iceberg, Apache Hudi; platforms like
  Databricks, Microsoft Fabric, Snowflake (Iceberg tables), BigQuery BigLake.

## 3. Star schema, facts/dimensions, SCD Type 2

**What it is.** A **star schema** has one central **fact table** of numeric events (sales) surrounded by
**dimension tables** (date, customer, product, store) that describe them. The **grain** says what one fact
row means. **SCD Type 2** (slowly changing dimension) keeps history: when a tracked attribute changes, the old
row is closed (`effective_to`, `is_current = false`) and a new row with a new **surrogate key** is inserted.
Type 1 just overwrites.

**Analogy.** A shop receipt (fact) with a product catalogue, a calendar and a customer card (dimensions).
SCD2 is like keeping every old address of a customer in the address book with "valid from/to" dates instead of
crossing it out.

**In this repo.** `sql/gold/05_fact_sales.sql` (grain = one **order line**, key `order_item_id`),
`sql/gold/01_dim_date.sql`, `02_dim_product.sql` and `03_dim_store.sql` (Type 1), and
`sql/gold/04_dim_customer_scd2.sql` (Type 2 via Delta `MERGE`). Facts join to the customer version valid
**at order time** (point-in-time join in `05_fact_sales.sql`). Test: `tests/test_scd2.py`.
In the run, 25 customers "moved"; 23 got a new version - 2 were randomly assigned the city they already had,
so correctly **no** new version.

**Interview Q&A**
- *Q: What is the grain of your fact table and why does it matter?* A: One row per order line
  (`order_item_id`). Declaring the grain first stops double counting - e.g. if I stored order totals on every
  line, summing would multiply revenue by the number of lines.
- *Q: How did you implement SCD2?* A: In one Delta `MERGE`. I find customers that are new or whose tracked
  attributes (city, state) changed by comparing a hash. Changed customers are fed to the MERGE twice: once with
  their key so the current row is matched and closed, and once with a NULL key so it never matches and a new
  version is inserted with a new surrogate key and `effective_from` = change time. It is idempotent - running
  it again with the same data changes nothing.

## 4. Medallion architecture (bronze / silver / gold)

**What it is.** A layered lake design. **Bronze** = raw data exactly as received, append-only, with audit
columns. **Silver** = cleaned, deduplicated, standardised, conformed. **Gold** = business-ready models
(star schemas, aggregates) for BI and analytics.

**Analogy.** Raw crude oil (bronze) -> refined fuel (silver) -> petrol at the pump, ready to use (gold).

**In this repo.** Bronze: `src/ingest_jdbc.py` adds `_ingested_at`, `_source`, `_batch_id`, `_load_type`
and partitions by `ingest_date`. Silver: `src/silver.py` (`dedupe_latest`, `standardise_city`,
`mask_email`, `mask_phone`, `split_valid`). Gold: `sql/gold/*.sql`. Rejected rows go to `lake/quarantine/`.

**Interview Q&A**
- *Q: Why keep bronze if silver is the clean copy?* A: Bronze is the replayable source of truth. If I find a
  bug in cleaning, I fix the code and rebuild silver/gold from bronze - no need to hit the legacy system again,
  which may be slow or already decommissioned after migration.
- *Q: What happens to bad rows?* A: They are not dropped silently. `split_valid` tags each row with all rules
  it fails; failures go to `lake/quarantine/<table>` with a `_reject_reason` (e.g. "unparseable order_date",
  "non-positive quantity"). Validation then proves source keys = silver + quarantine.

## 5. Data migration: 6 Rs, validation, cutover

**What it is.** The **6 Rs** are migration strategies: **Rehost** (lift-and-shift), **Replatform**
(lift-tinker-shift, e.g. to a managed DB), **Refactor/Re-architect** (redesign for cloud), **Repurchase**
(move to SaaS), **Retire**, **Retain**. A data migration needs **validation** (row counts, checksums,
aggregate reconciliation, sample checks) and a **cutover** plan: full load, then incremental (CDC) loads to
catch up, a short freeze, final delta load, validation sign-off, switch users, keep rollback ready.

**Analogy.** Moving house: you can move the furniture as-is (rehost), buy new furniture that fits better
(refactor), or throw some away (retire). Before giving back the old keys, you check every box arrived (validation).

**In this repo.** The analytics side is a **re-architecture** from an OLTP DB to a lakehouse.
Full load + CDC catch-up: `make ingest` then `make cdc`. Validation: `src/validate_migration.py` -
`checksum()` (order-independent SHA-256 per table), `run()` (row counts, totals, row accounting,
per-month revenue computed independently in `SOURCE_MONTHLY_SQL`), report in `reports/migration_validation.md`.

**Interview Q&A**
- *Q: How do you prove a migration is correct?* A: Layered checks. (1) Row counts per table source vs target.
  (2) A checksum over all columns - I hash every row with SHA-256 and sum the hashes, so order doesn't matter
  and one changed value changes the total. (3) Business totals, e.g. total amount and revenue per month.
  (4) Accounting: every source key is either in silver or in quarantine. All 17 checks PASS in my run.
- *Q: How would you do the cutover?* A: Initial full load weeks before, then scheduled CDC loads so the lake
  stays a few minutes behind. On cutover day: freeze writes in the legacy app, run a last incremental load,
  run the validation report, get business sign-off, point reports to the new platform, keep the old system
  read-only for a rollback window.

## 6. Data governance: quality, lineage, catalog, PII, RBAC, DPDP Act

**What it is.** Governance = making data trustworthy, findable and legally safe. **Data quality** checks
completeness, uniqueness, validity, consistency, referential integrity. **Lineage** shows where every table
comes from. A **catalog** documents tables/columns, owners and sensitivity. **PII** (personal data) is masked,
hashed or tokenised; **role-based access** gives each role only what it needs. India's **Digital Personal Data
Protection (DPDP) Act, 2023** requires purpose limitation, data minimisation, security safeguards, consent,
and breach reporting, with large penalties.

**Analogy.** A library: books are checked for missing pages (quality), each has a record of where it came from
(lineage), a card catalogue (catalog), and the rare-books room needs a special pass (RBAC).

**In this repo.**
- Quality: `src/quality_checks.py` (`completeness`, `uniqueness`, `validity`, `ref_integrity`) ->
  `reports/data_quality.md` (21/21 PASS).
- Lineage: `src/lineage.py` (`PIPELINE_EDGES` + `sql_edges()` parses the gold SQL) -> `reports/lineage.json`,
  `reports/lineage.md` (Mermaid).
- Catalog: `src/lineage.py` (`write_catalog`) -> `docs/data_catalog.md` with PII flag and owner per column.
- PII: `src/silver.py` `mask_email` (SHA-256) and `mask_phone` (last 4 digits).
- RBAC: `sql/gold/06_v_sales_analyst.sql` - a PII-free view for analysts, with the GRANT statements you'd run
  on Unity Catalog / Synapse. Power BI exports also exclude PII (`src/export_bi.py`).

**Interview Q&A**
- *Q: How did you handle PII?* A: Emails are SHA-256 hashed after lower-casing and trimming, so analysts can
  still count or join customers but can't read the email; phones keep only the last 4 digits. Analysts get a
  view without names. Under the DPDP Act a hash is still personal data (pseudonymised), so the raw bronze layer
  would be restricted to the ingestion team and retention-limited.
- *Q: What is data lineage and why does a migration need it?* A: Lineage is the map from source columns to
  report numbers. In a migration it lets you do impact analysis (what breaks if a source column changes) and
  answer auditors' "where did this number come from". I generate it from the pipeline and by parsing the SQL;
  in production tools like Purview, Unity Catalog or OpenLineage capture it automatically.

## 7. Spark: lazy evaluation, DAG, partitions, transformations vs actions

**What it is.** Spark is a distributed compute engine. **Transformations** (`filter`, `select`, `join`,
`groupBy`) only build a plan; **actions** (`count`, `show`, `collect`, `write`) run it - that is **lazy
evaluation**. The plan is a **DAG** (directed acyclic graph) of stages, split at **shuffles** (data moving
between machines for joins/groupBy). Data is split into **partitions**; one task processes one partition, so
partitions = parallelism. Narrow transformations (filter) need no shuffle; wide ones (groupBy, join) do.

**Analogy.** A recipe: writing the recipe steps costs nothing (transformations); cooking starts only when a guest
orders (action). Partitions are like splitting a big order across several cooks.

**In this repo.** `src/common.py` (`get_spark`: `local[*]`, `spark.sql.shuffle.partitions = 8`).
`src/ingest_jdbc.py` reads with `numPartitions` (4 parallel JDBC connections). `src/silver.py` caches a
DataFrame in `split_valid` because the same rows feed two writes (silver + quarantine).
`src/mapreduce_sales_by_region.py` prints the RDD lineage (`toDebugString`) - Spark's DAG.

**Interview Q&A**
- *Q: What is lazy evaluation and why is it useful?* A: Spark waits until an action to execute, so it can
  optimise the whole plan - push filters down to the source (my JDBC `WHERE updated_at > watermark` runs in
  Postgres), prune columns, and pick join strategies like broadcasting small tables.
- *Q: What is a shuffle and why is it expensive?* A: Rows with the same key must end up in the same
  partition, so data moves over the network and to disk. Wide operations like `groupBy`/`join` cause it.
  I reduced cost by lowering shuffle partitions for small data and by using `reduceByKey`, which pre-aggregates
  before the shuffle.

## 8. Cloud service mapping

**What it is.** The same architecture exists on every cloud with different names: object storage
(S3 / ADLS Gen2 / GCS), managed Spark (EMR / Databricks / Dataproc), orchestration & ingestion
(Glue / Data Factory / Dataflow), warehouse (Redshift / Synapse / BigQuery), streaming (MSK / Event Hubs /
Pub/Sub), catalog/governance (Glue Catalog / Purview / Dataplex), BI (QuickSight / Power BI / Looker).

**Analogy.** Same car, three dealerships - different badges, same engine parts.

**In this repo.** `config/local.yaml` vs `config/aws.yaml`, `azure.yaml`, `gcp.yaml`: only `lake_root`
(`s3a://`, `abfss://`, `gs://`) and connection strings change; `src/common.py` `lake_path()` builds every path
from it. Full table and an Azure target diagram: `docs/CLOUD_MAPPING.md`. (Mapped, **not deployed**.)

**Interview Q&A**
- *Q: How would you move this to Azure?* A: ADLS Gen2 for the lake, Data Factory with a self-hosted
  integration runtime to pull from the on-prem Postgres, Databricks to run the same PySpark/SQL, Event Hubs'
  Kafka endpoint for streaming, Unity Catalog + Purview for governance, Key Vault for secrets, Power BI on top.
  The code change is the config file.
- *Q: Which is cheaper, storage or compute?* A: Storage is cheap; compute is the main cost, so we separate
  them (lake storage + on-demand clusters that auto-terminate), partition data to read less, and use
  incremental loads instead of full reloads.

## 9. Schema-on-read vs schema-on-write

**What it is.** **Schema-on-write**: the structure is enforced when data is written (a database rejects a
bad row). **Schema-on-read**: raw files are stored as-is and a structure is applied when you query them.
Lakes use schema-on-read for raw zones; curated layers enforce schemas again.

**Analogy.** Schema-on-write = a form with mandatory boxes; schema-on-read = a pile of letters you
interpret when you read them.

**In this repo.** `src/register_tables.py`: `bronze.supplier_master` is a HiveQL external table over a raw CSV
folder (schema applied at query time); `ingest_supplier_json` reads two JSON files whose schemas differ (March
adds `transport` struct, `batch_codes` array, `temperature_c`) and merges them with Delta `mergeSchema` - the
plain append fails (shown in the output). The streaming consumer uses an explicit schema (`EVENT_SCHEMA`) =
schema-on-write for the stream.

**Interview Q&A**
- *Q: What is schema drift and how did you handle it?* A: When the source adds/removes/changes fields.
  Delta rejects the new columns by default (schema enforcement); I allowed it explicitly with `mergeSchema`,
  so old rows get NULLs in new columns. In production you'd alert on drift and review before evolving.
- *Q: Pros and cons of schema-on-read?* A: Fast, flexible ingestion and you never lose data; but errors are
  found late (at query time) and every reader must interpret the data the same way - which is why silver
  enforces types.

## 10. Hadoop: HDFS, YARN, MapReduce

**What it is.** Hadoop is the original open-source big-data platform. **HDFS** stores files split into
128 MB **blocks**, each **replicated** 3 times across **DataNodes**; the **NameNode** holds the metadata
(which blocks make up a file and where they are). **YARN** allocates CPU/RAM on the cluster to jobs.
**MapReduce** is the compute model: map -> shuffle/sort -> reduce, writing to disk between steps.
Spark replaced MapReduce for most work because it keeps data in memory.

**Analogy.** HDFS = a book torn into chapters with photocopies in three libraries, plus an index card
(NameNode) saying where each chapter is. YARN = the room booking system. MapReduce = everyone counts words in
their chapter (map), then counts for each word are added up (reduce).

**In this repo.** `docs/HADOOP_MAPPING.md` maps `lake/` to HDFS paths and `local[*]` to YARN.
`src/mapreduce_sales_by_region.py`: `map` -> `(region, net_amount)`, `reduceByKey` -> sum per region.

**Interview Q&A**
- *Q: Why is Spark faster than MapReduce?* A: MapReduce writes intermediate results to HDFS between every
  map and reduce; Spark keeps them in memory, builds an optimised DAG of the whole job, and recovers lost data
  from lineage instead of re-reading disk. It matters most for iterative and multi-step jobs.
- *Q: What happens if a DataNode fails?* A: Its blocks still exist on two other nodes (replication 3). The
  NameNode notices missing heartbeats and re-replicates those blocks elsewhere; running tasks are retried on
  nodes that hold a copy.

## 11. Hive and the metastore

**What it is.** **Hive** puts SQL on top of files in HDFS/object storage. The **metastore** is a database of
metadata: table names, columns, types, partitions and the file **location**. **External tables** point at
existing files - dropping the table keeps the data; **managed tables** own their data. Spark, Presto/Trino and
Databricks all use a Hive-compatible metastore (or Glue Catalog / Unity Catalog).

**Analogy.** The metastore is a library catalogue: it doesn't hold the books, it tells you which shelf they are on.

**In this repo.** `src/common.py` enables Hive support with an embedded **Derby** metastore in
`lake/_metastore`; `src/register_tables.py` (`register_bronze`) creates databases `bronze/silver/gold` and
external tables (`type=EXTERNAL` in the output) including a classic `ROW FORMAT DELIMITED ... STORED AS
TEXTFILE` table. A fallback to Spark's in-memory catalog exists in `create_spark()` but was not needed.

**Interview Q&A**
- *Q: External vs managed table?* A: For a managed table the metastore owns data and metadata - DROP deletes
  the files. For an external table it only owns metadata; DROP keeps the files. I used external tables
  because the lake folders are the source of truth and several engines may read them.
- *Q: Why does Derby not work in production?* A: Embedded Derby allows one connection (one Spark app) at a
  time. Production uses a shared metastore service on MySQL/Postgres, or a managed catalog like AWS Glue
  Catalog or Unity Catalog.

## 12. Kafka

**What it is.** Kafka is a distributed, durable **event log**. Producers write events to a **topic**; a topic
has **partitions** (ordered logs) for parallelism; each event has an **offset** (its position). Events with the
same **key** go to the same partition, so order is kept per key. **Consumer groups** share partitions among
members and remember committed offsets so they can resume. Events are retained for days, so consumers can replay.
**KRaft** mode runs Kafka without ZooKeeper.

**Analogy.** A WhatsApp group with numbered messages that are never deleted for a week: anyone can scroll back
(replay), and each reader remembers the last message number they read (offset).

**In this repo.** `docker-compose.yml` (single-node Kafka 3.9 in KRaft mode), `make kafka-up` creates topic
`retail.events` with 3 partitions. `src/stream_producer.py` (`make_event`, key = customer_id, `acks=all`).
`src/stream_consumer.py`: Spark Structured Streaming reads the topic, parses JSON with `EVENT_SCHEMA`,
`withWatermark("event_time", "10 minutes")`, 5-minute `window`, writes Delta with **checkpoints**.
Real output: `reports/streaming_demo_output.txt`.

**Interview Q&A**
- *Q: How do you get exactly-once results with Kafka + Spark?* A: Spark stores the offsets it processed in a
  checkpoint and writes to Delta transactionally; on restart it resumes from the checkpointed offsets, and the
  Delta sink ignores a batch it already committed. So the output is exactly-once even if delivery is at-least-once.
- *Q: What is a watermark?* A: How long to wait for late events. With a 10-minute watermark, a 5-minute window
  is finalised once event time passes window end + 10 minutes; events later than that are dropped and Spark
  can free the window's state.

## 13. Sqoop and CDC

**What it is.** **Sqoop** was the Hadoop tool for bulk-copying RDBMS tables into HDFS/Hive using parallel
mappers split on a column (`--split-by`, `--num-mappers`) and incremental imports (`--check-column`,
`--last-value`). It's retired; Spark JDBC does the same. **CDC (change data capture)** loads only what changed:
*query-based* CDC uses a timestamp/ID watermark; *log-based* CDC (Debezium, AWS DMS, Datastream) reads the
database's transaction log and also captures deletes.

**Analogy.** Instead of photocopying the whole register every night, you copy only the pages with today's date.

**In this repo.** `src/ingest_jdbc.py` (`ingest_table`): gets `min/max` of the split column, then reads with
`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` (4 parallel connections for orders). The
watermark (`max(updated_at)` per table) is saved in `lake/_state/watermarks.json`; the next run reads only
`updated_at > watermark`. Demo: `make cdc` -> the incremental run read 36 customers, 10 products, 0 stores,
250 orders, 400 order lines instead of ~172k rows.

**Interview Q&A**
- *Q: What are the limits of a timestamp watermark?* A: It misses hard DELETEs, relies on every app updating
  `updated_at`, and can miss rows committed late with an older timestamp - fixed with an overlap window plus
  dedupe on the key. Log-based CDC avoids all three.
- *Q: How does the parallel JDBC read work?* A: Spark splits the range [lowerBound, upperBound] of a numeric
  column into numPartitions slices and opens one connection per slice with a WHERE range. Bounds only set slice
  sizes - rows outside are still read by the first/last slice - so choose an evenly distributed column.

## 14. Delta Lake: ACID and time travel

**What it is.** Delta Lake adds a **transaction log** (`_delta_log/` JSON files) on top of Parquet. Each
write is an atomic commit, giving **ACID** (all-or-nothing writes, readers never see half-written data,
concurrent writers are checked). Every commit is a **version**, so you can query or restore the past
(**time travel**). It also supports `MERGE`, `UPDATE`, `DELETE`, schema enforcement and evolution.

**Analogy.** Git for tables: every write is a commit, and you can check out an older version.

**In this repo.** Every table is Delta. `src/validate_migration.py` (`time_travel`) runs
`DESCRIBE HISTORY gold.dim_customer` and `SELECT ... VERSION AS OF n`: v0 CREATE (0 rows), v1 first MERGE
(2,000 rows), v2 CDC MERGE (2,033 rows, 2,010 current). Look inside `lake/gold/dim_customer/_delta_log/`.

**Interview Q&A**
- *Q: What does ACID mean for a data lake?* A: Atomicity - a failed job leaves no partial files visible;
  Consistency - schema is enforced; Isolation - readers see a consistent snapshot while a write happens;
  Durability - committed data stays. Plain Parquet folders give none of that.
- *Q: Use cases for time travel?* A: Audits ("what did the report show last Tuesday"), reproducing an ML
  training set, debugging a bad load, and rollback with `RESTORE TABLE ... TO VERSION AS OF n`.

## 15. BI: Power BI, DAX, SQL window functions

**What it is.** BI tools (Power BI, Tableau, Qlik) turn a star schema into dashboards. Power BI uses
relationships (1-to-many from dimension to fact) and **DAX** measures computed in filter context.
SQL **window functions** (`RANK`, `LAG`, `SUM() OVER`) compute across related rows without collapsing them -
used for rankings, growth and running totals.

**Analogy.** The star schema is the ingredients, DAX measures are recipes, the dashboard is the plated dish.

**In this repo.** `sql/analytics/` - Q1 monthly trend with `LAG` (MoM growth), Q2 top products per region with
`RANK`, Q3 retention cohorts, Q4 store YoY with `LAG`, Q5 basket size. Run by `src/analytics.py`.
Dashboard: `src/dashboard.py` -> `dashboard/index.html`. Power BI rebuild, relationships and the 3 DAX measures:
`docs/POWERBI.md`.

**Interview Q&A**
- *Q: RANK vs DENSE_RANK vs ROW_NUMBER?* A: For values 100, 100, 90: RANK gives 1, 1, 3; DENSE_RANK gives
  1, 1, 2; ROW_NUMBER gives 1, 2, 3 (no ties). I used ROW_NUMBER to dedupe (exactly one row per key) and RANK
  for "top 3 products" so ties are shown fairly.
- *Q: How would you calculate YoY % in DAX?* A: `DIVIDE([Total Revenue] - CALCULATE([Total Revenue],
  SAMEPERIODLASTYEAR(dim_date[full_date])), CALCULATE(...same...))` - it needs a marked date table, and DIVIDE
  avoids divide-by-zero errors.

## 16. Scala

**What it is.** Scala is a JVM language mixing object-oriented and functional programming. Spark is written in
Scala, so the Scala API is native; PySpark DataFrame code becomes the same execution plan, so performance is
similar unless you use Python UDFs. Scala gives compile-time type safety (e.g. `Dataset[CaseClass]`).

**Analogy.** Spark's mother tongue; Python is a very good translator.

**In this repo.** `scala/GoldRevenue.scala` (run `make scala`) computes monthly revenue in Scala; its output
matches `sql/analytics/01_monthly_revenue_trend.sql` to the paisa (see README).

**Interview Q&A**
- *Q: PySpark or Scala - which is faster?* A: For DataFrame/SQL code, about the same: both compile to the same
  Catalyst plan running on the JVM. Python UDFs are slower because each row crosses Python <-> JVM; use built-in
  functions (as I did for masking with `sha2`) or pandas UDFs.
- *Q: `val` vs `var`?* A: `val` is immutable (can't be reassigned), `var` is mutable. Idiomatic Scala and Spark
  code prefers `val` - like Spark DataFrames, which are immutable.

---

## Bonus: questions about languages in the JD

- **Python**: all pipeline code. **SQL**: gold and analytics. **Java**: Spark, the Postgres JDBC driver, Kafka
  and Hive all run on the JVM (Java 21 here); JDBC itself is a Java API. **C/C++**: not used in this project -
  be honest: "I know the basics from coursework (pointers, memory management); Spark's Tungsten engine generates
  JVM bytecode, and Parquet/Arrow have C++ implementations."
