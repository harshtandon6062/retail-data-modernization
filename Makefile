# RetailCo legacy-to-lakehouse migration.  `make all` = everything, from scratch.
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c
PY := .venv/bin/python
# Hide Maven/Ivy download chatter that spark.jars.packages prints on every start
NOISE := ^	|^:: |^\s*$$|Ivy Default Cache|The jars for the packages|loading settings|Picked up JAVA_TOOL|incubator modules|WARN Utils|Setting default log level|To adjust logging level|^\s*(io|org|com|net)\.[a-z0-9.-]+\#
RUN = $(PY) -u
QUIET = 2>&1 | grep --line-buffered -vE '$(NOISE)'

.PHONY: all setup postgres legacy ingest register silver gold cdc governance validate bi mapreduce test \
        stream kafka-up kafka-down scala clean

all: setup postgres legacy ingest register silver gold cdc governance validate bi mapreduce test
	@echo -e "\n*** make all finished: see reports/, docs/data_catalog.md, dashboard/index.html ***"

setup: .venv/.installed
.venv/.installed: requirements.txt
	python3 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements.txt
	touch $@

postgres:            ## start PostgreSQL if it is not running (needs sudo/root locally)
	@pg_isready -q -h localhost || (service postgresql start || sudo service postgresql start)
	@sleep 1; pg_isready -h localhost

legacy:              ## 1. create the legacy OLTP source with dirty data
	$(RUN) src/generate_legacy_db.py

ingest:              ## 2. Sqoop-style parallel JDBC read -> bronze (full or CDC)
	$(RUN) src/ingest_jdbc.py $(QUIET)

register:            ## 3. Hive metastore tables + schema-on-read supplier files
	$(RUN) src/register_tables.py $(QUIET)

silver:              ## 4. clean / dedupe / mask PII / quarantine
	$(RUN) src/silver.py $(QUIET)

gold:                ## 5. star schema via SQL files (SCD2 MERGE)
	$(RUN) src/gold.py $(QUIET)

cdc:                 ## simulate new activity, then incremental load all the way to gold
	$(RUN) src/generate_legacy_db.py --apply-changes
	$(RUN) src/ingest_jdbc.py $(QUIET)
	$(RUN) src/silver.py $(QUIET)
	$(RUN) src/gold.py $(QUIET)

governance:          ## 6. quality checks, lineage, catalog
	$(RUN) src/quality_checks.py $(QUIET)
	$(RUN) src/lineage.py $(QUIET)

validate:            ## 7. source vs lake reconciliation + time travel
	$(RUN) src/validate_migration.py $(QUIET)

bi:                  ## 8. analytics SQL, Power BI exports, HTML dashboard
	$(RUN) src/analytics.py $(QUIET)
	$(RUN) src/export_bi.py $(QUIET)
	$(RUN) src/dashboard.py

mapreduce:           ## 12. MapReduce-style RDD job
	$(RUN) src/mapreduce_sales_by_region.py $(QUIET)

test:
	$(PY) -m pytest -q tests $(QUIET)

# ---- Tier 2: streaming (needs Docker) ----
kafka-up:
	docker compose up -d kafka
	@echo "waiting for Kafka..."; for i in $$(seq 1 30); do \
	  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 2; done
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic retail.events --partitions 3 --replication-factor 1

stream: kafka-up     ## producer + Spark Structured Streaming consumer (~1 minute)
	$(RUN) src/stream_producer.py --seconds 50 & \
	$(RUN) src/stream_consumer.py --seconds 75 $(QUIET); wait

kafka-down:
	docker compose down -v

scala:               ## 11. Scala version of the monthly revenue aggregation (spark-shell bundled with PySpark)
	@# `spark-shell -i file` is silently ignored by Spark 4's Scala 2.13 REPL when run
	@# non-interactively, so we feed `:paste file` on stdin instead (same effect).
	echo ':paste scala/GoldRevenue.scala' | .venv/lib/python3*/site-packages/pyspark/bin/spark-shell \
	  --master "local[*]" --packages io.delta:delta-spark_2.13:4.0.1 \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --conf spark.ui.showConsoleProgress=false \
	  --conf spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:config/log4j2.properties $(QUIET)

clean:               ## delete everything generated (lake, reports, exports, dashboard)
	rm -rf lake spark-warehouse metastore_db derby.log .pytest_cache
	rm -f reports/*.md reports/*.json reports/analytics/*.csv exports/powerbi/*.csv dashboard/index.html docs/data_catalog.md
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
