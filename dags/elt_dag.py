# dags/elt_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.extract import main as extract_main
from src.load_snowflake import main as load_main
import subprocess

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2025,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform():
    import snowflake.connector, json
    cfg = json.load(open("config/snowflake_config.json"))
    conn = snowflake.connector.connect(user=cfg["user"], password=cfg["password"], account=cfg["account"], warehouse=cfg["warehouse"], database=cfg["database"], schema=cfg["schema"])
    sql = open("src/transform_snowflake.sql").read()
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()

with DAG('weather_elt', default_args=default_args, schedule_interval='@daily') as dag:
    t1 = PythonOperator(task_id='extract', python_callable=extract_main)
    t2 = PythonOperator(task_id='load', python_callable=load_main)
    t3 = PythonOperator(task_id='transform', python_callable=transform)

    t1 >> t2 >> t3
