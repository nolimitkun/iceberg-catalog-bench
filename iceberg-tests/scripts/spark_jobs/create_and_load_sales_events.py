import os
import sys
import yaml
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
    DateType,
)
from pyspark.sql import functions as F
from dotenv import load_dotenv

# Ensure project root is on sys.path for 'scripts' imports
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.common.data_gen import generate_sales_events


def build_spark(app_name: str, conf: dict) -> SparkSession:
    print("[spark-job] Building SparkSession with conf keys:", list(conf.keys()))
    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    print("[spark-job] Spark version:", spark.version)
    return spark


def get_schema():
    return StructType([
        StructField("event_id", LongType(), False),
        StructField("tenant_id", IntegerType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("sku", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("price", DecimalType(18, 2), True),
        StructField("country", StringType(), True),
        StructField("ds", DateType(), True),
    ])


def main():
    # Load .env if present
    dotenv_path = os.path.join(ROOT_DIR, ".env")
    load_dotenv(dotenv_path, override=False)
    print("[spark-job] Loaded .env from:", dotenv_path if os.path.exists(dotenv_path) else "(not found)")

    if len(sys.argv) < 3:
        print("Usage: create_and_load_sales_events.py <engines.yaml> <datasets.yaml> [<namespace>]")
        sys.exit(1)

    engines_path = sys.argv[1]
    datasets_path = sys.argv[2]
    namespace = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("INTEROP_NAMESPACE", "interopspec")

    with open(engines_path, "r") as f:
        engines_text = f.read()
    engines_text = os.path.expandvars(engines_text)
    engines = yaml.safe_load(engines_text)

    with open(datasets_path, "r") as f:
        datasets = yaml.safe_load(f)

    spark_conf = dict(engines["spark"]["conf"])  # copy so we can adjust
    print("[spark-job] Catalog conf:", engines["spark"].get("catalog", {}))

    catalog_name = engines["spark"]["catalog"]["name"]

    spark = build_spark("CreateAndLoadSalesEvents", spark_conf)

    dataset_conf = datasets["datasets"]["small_sales_events"]

    # Use Iceberg catalog-qualified identifiers or namespace-only per user's config
    ns_qualified = f"{namespace}"
    table_identifier = f"{ns_qualified}.sales_events"
    print("[spark-job] Namespace:", ns_qualified)
    print("[spark-job] Table identifier:", table_identifier)

    spark.sql(f"SHOW NAMESPACES").show(truncate=False)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns_qualified}")

    spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")

    print("[spark-job] Creating table with Iceberg...")
    spark.sql(
        f"""
        CREATE TABLE {table_identifier} (
          event_id BIGINT,
          tenant_id INT,
          event_ts TIMESTAMP,
          sku STRING,
          qty INT,
          price DECIMAL(18,2),
          country STRING,
          ds DATE
        )
        USING iceberg
        PARTITIONED BY (days(event_ts), bucket(16, tenant_id))
        TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='hash', 'commit.manifest.min-count-to-merge'='100')
        """
    )

    print("[spark-job] Generating rows:", dataset_conf["rows"])
    rows = generate_sales_events(dataset_conf["rows"], datetime.utcnow())
    # Convert price floats to Decimal(18,2) to match schema
    for r in rows:
        r["price"] = Decimal(str(r["price"])) .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    print("[spark-job] Creating DataFrame...")
    df = spark.createDataFrame(rows, schema=get_schema())
    print("[spark-job] DataFrame count:", df.count())

    df = df.repartition(64, F.col("tenant_id"))

    print("[spark-job] Appending data to:", table_identifier)
    df.writeTo(table_identifier).append()   

    print("[spark-job] Done. Table created and data loaded:", table_identifier)


if __name__ == "__main__":
    main()
