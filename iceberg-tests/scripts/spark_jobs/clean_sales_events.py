import os
import sys
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build_spark(app_name: str, conf: dict) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def main():
    load_dotenv(os.path.join(ROOT_DIR, ".env"), override=False)

    if len(sys.argv) < 3:
        print("Usage: clean_sales_events.py <engines.yaml> <datasets.yaml> [<namespace>]")
        sys.exit(1)

    engines_path = sys.argv[1]
    datasets_path = sys.argv[2]
    namespace = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("INTEROP_NAMESPACE", "interopspec")

    with open(engines_path, "r") as f:
        engines_text = os.path.expandvars(f.read())
        engines = yaml.safe_load(engines_text)

    spark_conf = dict(engines["spark"]["conf"])  # copy

    spark = build_spark("CleanSalesEvents", spark_conf)

    ns = namespace
    table = f"{ns}.sales_events"

    print(f"[clean] Dropping table if exists: {table}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    print(f"[clean] Dropping namespace if empty: {ns}")
    try:
        spark.sql(f"DROP NAMESPACE IF EXISTS {ns}")
    except Exception as e:
        print(f"[clean] Namespace drop skipped: {e}")

    print("[clean] Done")


if __name__ == "__main__":
    main()
