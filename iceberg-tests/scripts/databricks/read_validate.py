import os
import sys
import yaml
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    load_dotenv(os.path.join(ROOT_DIR, ".env"), override=False)

    if len(sys.argv) < 3:
        print("Usage: read_validate.py <engines.yaml> <datasets.yaml> [<namespace>]")
        sys.exit(1)

    engines_path = sys.argv[1]
    datasets_path = sys.argv[2]
    namespace = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("INTEROP_NAMESPACE", "interopspec")

    with open(engines_path, "r") as f:
        engines_text = os.path.expandvars(f.read())
        engines = yaml.safe_load(engines_text)

    table = f"{namespace}.sales_events"
    print(f"[databricks] Placeholder read_validate for table: {table}")
    print("[databricks] Implement Databricks Jobs/UC to read and count rows here.")


if __name__ == "__main__":
    main()
