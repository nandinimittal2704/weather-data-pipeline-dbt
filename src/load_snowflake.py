import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from utils import read_json
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

# Get the project root (parent of src directory)
PROJECT_ROOT = Path(__file__).parent.parent

# Read config and allow environment variables to override secrets
CFG = read_json(str(PROJECT_ROOT / "config" / "snowflake_config.json"))
# Override with environment variables when provided (do not store secrets in repo)
for k, env_key in [("password", "SNOWFLAKE_PASSWORD"), ("user", "SNOWFLAKE_USER"), ("account", "SNOWFLAKE_ACCOUNT")]:
    v = os.getenv(env_key)
    if v:
        CFG[k] = v

def connect_snowflake(cfg=CFG):
    # Validate that credentials are not placeholders before making network calls
    def _is_placeholder(v: str) -> bool:
        if not v:
            return True
        up = str(v).strip().upper()
        return up.startswith("YOUR") or up.startswith("REPLACE")

    missing = [k for k in ("user", "password", "account") if _is_placeholder(cfg.get(k))]
    if missing:
        raise RuntimeError(
            "Snowflake credentials appear to be missing or are placeholders: "
            + ", ".join(missing)
            + ".\nSet real values in .env (SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT) or in config/snowflake_config.json"
        )

    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg.get("warehouse"),
        database=cfg.get("database"),
        schema=cfg.get("schema")
    )
    return conn

def create_raw_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS WEATHER_RAW (
      CITY STRING,
      TIMESTAMP NUMBER,
      TEMP_C FLOAT,
      HUMIDITY NUMBER,
      WEATHER_MAIN STRING,
      RAW VARIANT
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        print("Ensured WEATHER_RAW exists")

def load_df_to_snowflake(df, conn, table="WEATHER_RAW"):
    df2 = df[["city", "timestamp", "temp_c", "humidity", "weather_main", "raw"]].copy()
    # Convert column names to uppercase for Snowflake
    df2.columns = [col.upper() for col in df2.columns]
    success, nchunks, nrows, _ = write_pandas(conn, df2, table)
    print(f"Loaded to Snowflake: success={success}, nrows={nrows}")

def main():
    try:
        df = pd.read_json(str(PROJECT_ROOT / "data" / "sample_weather.json"))
        print(f"Successfully loaded {len(df)} rows from data file")
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Attempt Snowflake connection
        print("\nAttempting Snowflake connection...")
        conn = connect_snowflake()
        create_raw_table(conn)
        load_df_to_snowflake(df, conn)
        conn.close()
        print("✓ Load completed successfully!")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        print("\nNote: Ensure valid Snowflake credentials are configured in config/snowflake_config.json")
        raise

if __name__ == "__main__":
    main()
