import os
import sys
import yaml
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


def _load_private_key(private_key_path: str, passphrase: str = None):
    """Load private key from file for key pair authentication."""
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )
    return private_key


def run_sql(statements: list[str], conn_params: dict):
    conn_kwargs = {
        "account": conn_params["account"],
        "user": conn_params["user"],
        "role": conn_params.get("role"),
        "warehouse": conn_params.get("warehouse"),
    }
    
    # Handle RSA key pair vs PAT vs password authentication
    if conn_params.get("private_key_path"):
        private_key_path = conn_params["private_key_path"]
        private_key_passphrase = conn_params.get("private_key_passphrase")
        print("[snowflake] Using RSA key pair authentication")
        conn_kwargs["private_key"] = _load_private_key(private_key_path, private_key_passphrase)
    elif conn_params.get("token"):
        conn_kwargs["token"] = conn_params["token"]
        print("[snowflake] Using PAT authentication")
    else:
        conn_kwargs["password"] = conn_params["password"]
        print("[snowflake] Using password authentication")
    
    ctx = snowflake.connector.connect(**conn_kwargs)
    try:
        cs = ctx.cursor()
        try:
            for stmt in statements:
                print(f"Executing: {stmt}")
                cs.execute(stmt)
        finally:
            cs.close()
    finally:
        ctx.close()


def main():
    # Load .env if present
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    load_dotenv(os.path.join(root_dir, ".env"), override=False)

    if len(sys.argv) < 3:
        print("Usage: run_sql.py <engines.yaml> <sql_file>")
        sys.exit(1)

    engines_path = sys.argv[1]
    sql_file = sys.argv[2]

    with open(engines_path, "r") as f:
        engines = yaml.safe_load(f)
    snow = engines["snowflake"]

    # Allow override by env vars
    snow = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", snow.get("account")),
        "user": os.getenv("SNOWFLAKE_USER", snow.get("user")),
        "password": os.getenv("SNOWFLAKE_PASSWORD", snow.get("password")),
        "token": os.getenv("SNOWFLAKE_TOKEN"),
        "private_key_path": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        "private_key_passphrase": os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        "role": os.getenv("SNOWFLAKE_ROLE", snow.get("role")),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", snow.get("warehouse")),
    }

    with open(sql_file, "r") as f:
        sql_text = f.read()

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    run_sql(statements, snow)


if __name__ == "__main__":
    main()
