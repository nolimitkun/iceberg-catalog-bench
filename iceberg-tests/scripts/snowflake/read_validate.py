import os
import sys
import yaml
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives import serialization

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_private_key(private_key_path: str, passphrase: str = None):
    """Load private key from file for key pair authentication."""
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )
    return private_key


def _normalize_account_and_host(account_val: str | None, host_val: str | None):
    # If host is provided, prefer it directly
    if host_val:
        host_val = host_val.strip()
        # Strip protocol if present
        if host_val.startswith("https://"):
            host_val = host_val[len("https://"):]
        if host_val.endswith("/"):
            host_val = host_val[:-1]
        return None, host_val
    # Otherwise, normalize account locator (no domain or protocol)
    if not account_val:
        return None, None
    acc = account_val.strip()
    # Accept forms like xy12345, xy12345.us-east-1, orgname-accountname
    if "snowflakecomputing.com" in acc:
        # Extract subdomain part before snowflakecomputing.com
        acc = acc.replace("https://", "").replace("http://", "")
        acc = acc.split("snowflakecomputing.com")[0]
        acc = acc.rstrip("./:")
        # If like vs41448., drop trailing dot
        if acc.endswith("."):
            acc = acc[:-1]
    return acc, None


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

    snow = engines["snowflake"]

    raw_account = os.getenv("SNOWFLAKE_ACCOUNT", snow.get("account"))
    raw_host = os.getenv("SNOWFLAKE_HOST")
    account, host = _normalize_account_and_host(raw_account, raw_host)

    conn_kwargs = {
        "user": os.getenv("SNOWFLAKE_USER", snow.get("user")),
        "role": os.getenv("SNOWFLAKE_ROLE", snow.get("role")),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", snow.get("warehouse")),
        "client_session_keep_alive": True,
    }
    
    # Check for RSA key pair authentication first
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        print("[snowflake] Using RSA key pair authentication")
        conn_kwargs["private_key"] = _load_private_key(private_key_path, private_key_passphrase)
    # Check for PAT authentication second
    elif os.getenv("SNOWFLAKE_TOKEN"):
        snowflake_token = os.getenv("SNOWFLAKE_TOKEN")
        print("[snowflake] Using PAT authentication")
        conn_kwargs["token"] = snowflake_token
    else:
        # Fall back to password authentication
        snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", snow.get("password"))
        if not snowflake_password:
            print("[snowflake] ERROR: No SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_TOKEN, or SNOWFLAKE_PASSWORD provided")
            sys.exit(1)
        conn_kwargs["password"] = snowflake_password
        print("[snowflake] Using password authentication")
    
    if host:
        conn_kwargs["host"] = host
    else:
        conn_kwargs["account"] = account

    print("[snowflake] Connecting with:", {k: v for k, v in conn_kwargs.items() if k not in ["password", "private_key", "token"]})
    ctx = snowflake.connector.connect(**conn_kwargs)
    database = os.getenv("SNOWFLAKE_DATABASE", snow.get("database"))

    table = f"catalog_linked_table"
    use_database_stmt = f"USE DATABASE {database}"
    use_schema_stmt = f"USE SCHEMA {namespace}"
    show_schemas_stmt = "SHOW SCHEMAS"
    show_tables_stmt = "SHOW TABLES"
    select_stmt = f"SELECT COUNT(1) AS cnt FROM {table}"

    try:
        cs = ctx.cursor(DictCursor)
        try:
            print("[snowflake] Running:", use_database_stmt)
            cs.execute(use_database_stmt)
            print("[snowflake] Running:", show_schemas_stmt)
            cs.execute(show_schemas_stmt)
            schema_rows = cs.fetchmany(10)
            schema_names = [
                row.get("name") or row.get("NAME") or row.get("SCHEMA_NAME")
                for row in schema_rows if row
            ]
            print("[snowflake] Schemas (first 10):", schema_names)
            print("[snowflake] Running:", use_schema_stmt)
            cs.execute(use_schema_stmt)
            print("[snowflake] Running:", show_tables_stmt)
            cs.execute(show_tables_stmt)
            table_rows = cs.fetchmany(10)
            table_names = [
                row.get("name") or row.get("NAME") or row.get("TABLE_NAME")
                for row in table_rows if row
            ]
            print("[snowflake] Tables (first 10):", table_names)
            print("[snowflake] Running:", select_stmt)
            cs.execute(select_stmt)
            row = cs.fetchone()
            if row:
                count_val = (
                    row.get("cnt")
                    or row.get("CNT")
                    or row.get("COUNT")
                    or next(iter(row.values()))
                )
            else:
                count_val = None
            print("[snowflake] Row count:", count_val)
        finally:
            cs.close()
    finally:
        ctx.close()


if __name__ == "__main__":
    main()
