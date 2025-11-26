# ELT Weather Project — Python + Snowflake

## Objective
Learn the ETL flow:
E = Extract  →  Weather API → JSON
L = Load     →  Snowflake Warehouse
T = Transform → SQL inside Snowflake

## Order of execution
1. Run extract.py  → Creates JSON file in /data
2. Run load_snowflake.py → Loads JSON rows into WEATHER_RAW table
3. Run transform_snowflake.sql → Creates ANALYTICS.WEATHER_CLEAN table

## Skills learned
✔ API integration  
✔ JSON handling  
✔ Python data pipelines  
✔ Load data into Snowflake table  
✔ SQL transformation inside warehouse  

## Commands to run
bash script/run_full_pipeline.sh

## Final Data Tables
RAW.WEATHER_RAW          → Raw data
ANALYTICS.WEATHER_CLEAN  → Clean/Transformed data

## Next upgrades (learn once basics clear)
- Schedule with Airflow
- Use S3 instead of local JSON
- Create dashboards using Power BI or Tableau
