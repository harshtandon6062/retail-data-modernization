# Interview kit

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
