from etl_tasks import *

from airflow import DAG
from datetime import datetime
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())
shared_directory = os.getenv('SHARED_DIR', "/opt/airflow/shared")
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

    try:
        symbols = read_symbols_from_file(f"{shared_directory}/symbols.json")

        dates = generate_dates()

        combinations = make_combinations(symbols, dates)

        shareholders = fetch_shareholders.expand_kwargs(combinations)

        output_csv = save_to_csv(shareholders)

        successfully = upsert_data_to_postgres(output_csv)

        cleanup(successfully)
    except Exception as e:
        print(f"-------- {e} -------")


