#!/bin/bash
set -e
python src/extract.py
python src/load_snowflake.py
# transform via python
python - <<'PY'
import snowflake.connector
from src.utils import read_json
cfg = read_json("config/snowflake_config.json")
conn = snowflake.connector.connect(user=cfg["user"], password=cfg["password"], account=cfg["account"], warehouse=cfg["warehouse"], database=cfg["database"], schema=cfg["schema"])
sql = open("src/transform_snowflake.sql").read()
cur = conn.cursor()
cur.execute(sql)
print("Transformation ran")
cur.close()
conn.close()
PY
