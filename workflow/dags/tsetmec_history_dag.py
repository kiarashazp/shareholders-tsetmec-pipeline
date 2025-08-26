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
import logging
from etl_tasks import *

from airflow import DAG
from datetime import datetime
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())
logger = logging.getLogger("shareholders_pipeline")
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
        logger.info(f"✅ Loaded {len(symbols)} symbols")

        dates = generate_dates()
        logger.info(f"✅ Generated {len(dates)} working dates")

        combinations = make_combinations(symbols, dates)
        logger.info(f"✅ Created {len(combinations)} symbol-date combinations")

        shareholders = fetch_shareholders.expand_kwargs(combinations)
        logger.info(f"✅ finished fetch_shareholders")

        output_csv = save_to_csv.expand(records=shareholders)
        logger.info(f"✅ save csv ")

        successfully = upsert_data_to_postgres.expand(csv_path=output_csv)
        logger.info(f"✅ insert to postgres")

        cleanup(successfully)
        logger.info(f"✅ cleaned up")
    except Exception as e:
        logger.exception(f"❌ DAG failed due to unexpected error: {type(e)} - {e}")
        raise


