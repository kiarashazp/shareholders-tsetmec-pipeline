"""
Airflow DAG definition for the Shareholders ETL pipeline.

This DAG orchestrates the daily extraction, transformation, and loading (ETL)
of shareholder data from the TSETMC API into a PostgreSQL database.
The pipeline includes the following steps:
    1. Read symbol list from a JSON/CSV file in the shared directory.
    2. Generate the most recent 10 valid working dates (based on the Jalali calendar).
    3. Create all combinations of symbols and dates.
    4. Fetch shareholder data from the TSETMC API for each combination.
    5. Save the fetched records into CSV files.
    6. Upsert the data from CSV files into the PostgreSQL ETL database.
    7. Clean up temporary CSV files after successful load.
The DAG is scheduled to run daily and supports retries in case of transient failures.
"""

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

        output_csv = save_to_csv.expand(shareholders)

        successfully = upsert_data_to_postgres(output_csv)

        cleanup(successfully)
    except Exception as e:
        print(f"-------- {e} -------")


