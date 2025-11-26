#!/usr/bin/env python
"""
Complete ELT Pipeline: Extract → Load → Transform
"""
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

print("=" * 60)
print("Starting ELT Pipeline")
print("=" * 60)

# Step 1: Extract
print("\n[1/3] EXTRACT: Fetching weather data...")
from extract import main as extract_main
extract_main()

# Step 2: Load
print("\n[2/3] LOAD: Loading data to Snowflake...")
from load_snowflake import main as load_main
load_main()

# Step 3: Transform
print("\n[3/3] TRANSFORM: Running SQL transformations...")
import snowflake.connector
from utils import read_json

cfg = read_json(str(project_root / "config" / "snowflake_config.json"))
conn = snowflake.connector.connect(
    user=cfg['user'], 
    password=cfg['password'], 
    account=cfg['account'], 
    warehouse=cfg['warehouse'], 
    database=cfg['database'], 
    schema=cfg['schema']
)

sql = open(str(project_root / "src" / "transform_snowflake.sql")).read()
cur = conn.cursor()

# Snowflake Python connector doesn't allow executing multi-statement SQL via
# cursor.execute when the string contains several statements. Use
# cursor.execute_string() where available, otherwise split on ';' and run
# statements individually.
try:
    # execute_string runs multiple statements and returns status objects
    if hasattr(cur, 'execute_string'):
        cur.execute_string(sql)
    else:
        # naive fallback for simple scripts: split by semicolon
        for stmt in [s.strip() for s in sql.split(';') if s.strip()]:
            cur.execute(stmt)

    print("✓ Transformation completed successfully")
except Exception as e:
    print(f"Transformation error: {type(e).__name__}: {e}")
    raise
cur.close()
conn.close()

print("\n" + "=" * 60)
print("✓ ELT Pipeline completed successfully!")
print("=" * 60)
