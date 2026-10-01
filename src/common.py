"""Shared helpers: config, lake paths and one place that builds the SparkSession.

Every pipeline step imports `get_spark()` from here so that all steps use the
same settings (Delta Lake, Hive metastore, timezone, JDBC driver).
"""
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent

# Versions of the extra Java libraries Spark downloads from Maven Central.
SPARK_VERSION = "4.0.1"
DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.0.1"
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.7"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"


def load_config():
    """Read config/<env>.yaml. Switch environment with RETAILCO_ENV=aws|azure|gcp."""
    env = os.environ.get("RETAILCO_ENV", "local")
    with open(ROOT / "config" / f"{env}.yaml") as f:
        return yaml.safe_load(f)


CONFIG = load_config()


def lake_path(*parts):
    """Build a path inside the lake. Locally this is ROOT/lake/...; on cloud
    configs lake_root is s3a://, abfss:// or gs:// and nothing else changes."""
    root = CONFIG["lake_root"]
    if "://" not in root:
        root = str(ROOT / root)
    return "/".join([root.rstrip("/")] + [p.strip("/") for p in parts])


def jdbc_url():
    pg = CONFIG["postgres"]
    return f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"


def jdbc_props():
    pg = CONFIG["postgres"]
    return {"user": pg["user"], "password": pg["password"], "driver": "org.postgresql.Driver"}


def get_spark(app_name, hive=True, extra_packages=()):
    """Create a local SparkSession with Delta Lake (+ optional Hive metastore).

    local[*]  = run Spark inside this one process using all CPU cores.
                On a cluster this would be `yarn` or `k8s://...` instead.
    """
    from pyspark.sql import SparkSession

    packages = [DELTA_PACKAGE, POSTGRES_JDBC_PACKAGE, *extra_packages]
    metastore_dir = ROOT / "lake" / "_metastore"
    metastore_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        # Delta Lake: adds MERGE/UPDATE/DELETE + time travel to Spark SQL
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Small data -> few shuffle partitions (default 200 is for big clusters)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", str(ROOT / "lake" / "_warehouse"))
        # Hive metastore backed by an embedded Derby database (a real
        # deployment would point this at MySQL/Postgres or a cloud catalog).
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir / 'metastore_db'};create=true",
        )
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore_dir} -Duser.timezone=UTC")
    )
    if hive:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def banner(text):
    print("\n" + "=" * 70 + f"\n{text}\n" + "=" * 70, flush=True)
