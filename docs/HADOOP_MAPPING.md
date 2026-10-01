# How this project maps to Hadoop

This project runs Spark in **local mode** on one machine, writing to a local folder (`lake/`).
In a classic on-prem big-data platform the same code would run on a **Hadoop cluster**.
Nothing in the pipeline logic changes; only *where files live* and *who hands out CPU/RAM*.

## The three parts of Hadoop

| Hadoop part | What it does | In this repo |
|---|---|---|
| **HDFS** (storage) | Distributed file system: big files are split into blocks and spread over many machines | `lake/` folder on local disk (`config/local.yaml: lake_root`) |
| **YARN** (resource manager) | Decides which machine runs which task, with how much CPU/RAM | Spark `local[*]` = all cores of this one machine (`src/common.py: get_spark`) |
| **MapReduce** (compute) | Programming model: map -> shuffle/sort -> reduce, disk between stages | Replaced by Spark (in memory). A MapReduce-style job is in `src/mapreduce_sales_by_region.py` |

## HDFS in more detail

- **Blocks**: a file is cut into 128 MB blocks (default). `fact_sales` here is ~10 MB, so it would be 1 block;
  a 1 TB table would be ~8,000 blocks, processed by ~8,000 parallel tasks.
- **Replication**: each block is copied to 3 DataNodes (default factor 3). If a machine dies, the data still exists twice.
- **NameNode**: the "index" - knows which blocks make up each file and where each block lives. Holds metadata only.
  (Single point of failure in old Hadoop -> HA NameNode with a standby.)
- **DataNodes**: the machines that actually store blocks and serve reads/writes.
- **Data locality**: YARN tries to run each task *on the machine that already holds the block* - move compute to data, not data to compute.

| Local path in this repo | Equivalent on HDFS |
|---|---|
| `lake/bronze/orders/ingest_date=2026-10-01/part-*.parquet` | `hdfs://namenode:8020/retailco/bronze/orders/ingest_date=2026-10-01/part-*.parquet` |
| `lake/_metastore/metastore_db` (Derby) | Hive Metastore service backed by MySQL/PostgreSQL |
| `lake/_checkpoints/` (streaming) | `hdfs:///retailco/_checkpoints/` |

Switching would be one line: `lake_root: hdfs://namenode:8020/retailco` plus running with `--master yarn`.

## YARN vs Spark local mode

| | Spark local mode (this repo) | Spark on YARN |
|---|---|---|
| Start | `master("local[*]")` | `spark-submit --master yarn --deploy-mode cluster` |
| Driver | inside this Python process | in a YARN container (cluster mode) or on the client |
| Executors | threads in the same JVM | separate JVMs (containers) on many NodeManagers |
| Resources | all cores of one machine | `--num-executors 10 --executor-cores 4 --executor-memory 8g` requested from the ResourceManager |
| Good for | development, tests, demos, small data | production, large data, many users sharing a cluster |

## MapReduce vs Spark

| | Hadoop MapReduce | Spark |
|---|---|---|
| Between stages | writes intermediate results to **disk** (HDFS) | keeps them in **memory** where possible |
| Programming | only map + reduce; complex jobs = chain of jobs | rich API: SQL, DataFrames, joins, windows, streaming, ML |
| Iterative work (ML, graphs) | very slow (re-reads disk each pass) | fast (cache data in memory) |
| Fault tolerance | re-run failed task from HDFS input | re-compute lost partitions from the **lineage DAG** |
| Languages | Java (Hive/Pig on top) | Scala, Python, Java, SQL, R |

## The tiny MapReduce-style job

`src/mapreduce_sales_by_region.py` (run: `make mapreduce`) computes net sales per region with the RDD API:

```python
facts.map(lambda f: (region_of[f.store_key], f.net_amount))   # MAP     -> (key, value)
     .reduceByKey(lambda a, b: a + b)                          # SHUFFLE + REDUCE (with map-side combine)
```

Real output from this repo (`make mapreduce`):

```
NORTH  INR   278,642,926.02
WEST   INR   272,688,994.73
EAST   INR   189,686,986.74
SOUTH  INR   183,340,832.07
```

The same answer in SQL would be `SELECT region, SUM(net_amount) ... GROUP BY region` - Spark turns that into
the same map -> shuffle -> reduce plan internally.

## Where the other Hadoop-ecosystem tools appear

| Tool | Role | In this repo |
|---|---|---|
| **Hive** | SQL on files + the metastore (table -> files) | `src/register_tables.py` (HiveQL external table, Derby metastore) |
| **Sqoop** | bulk RDBMS -> HDFS import with parallel mappers | replaced by Spark JDBC parallel reads, `src/ingest_jdbc.py` |
| **Kafka** | event streaming | `docker-compose.yml`, `src/stream_*.py` |
| **Oozie / Airflow** | scheduling | `Makefile` (a real deployment would use Airflow / ADF / Step Functions) |
