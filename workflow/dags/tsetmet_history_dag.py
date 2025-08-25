from airflow import DAG
from datetime import datetime

from etl_tasks import *

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="shareholders_pipeline", default_args=default_args,
    description="ETL pipeline for shareholders data",
    start_date=datetime.now() - timedelta(days=30), schedule_interval="@daily",
    catchup=True, tags=["etl", "postgres", "api"]
) as dag:
    pass
