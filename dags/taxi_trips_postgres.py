import sys
import os

# Add the project root to PYTHONPATH
sys.path.append('/opt/airflow')

# import function from other modules in the parent directory (project repository)
from db_utils import get_client, get_postgres_engine, get_postgres_engine2, get_snowflake_engine
from extract_clickhouse import fetch_data
from load_data import load_csv_to_postgres, load_csv_to_snowflake
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# import airflow functions
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

# postgres engine
engine = get_postgres_engine()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pipeline logic for extraction and Staging
def fetch_data(execution_date, **kwargs):

    client = get_client()
    query = f'''
        select pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, trip_amount
        from tripdata
        where pickup_date = toDate('{execution_date}') + 1
        '''
    try:
        logger.info("Executing query: %s", query)
        fetch_data(client=client, query=query)
        logger.info("Data fetched successfully for date: %s", execution_date)
    except Exception as e:
        logger.error("Error fetching data: %s", e)
        raise

# staging fetched data
def staging_data(**kwargs):

    schema = 'stg'
    table_name = 'tripsdata'
    csv_file_path = '/opt/airflow/raw_data/tripsdata.csv'

    try:
        logger.info("Loading data from %s into %s.%s", csv_file_path, schema, table_name)
        load_csv_to_snowflake(csv_file_path, table_name, engine, schema)
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

def transform_and_load(**kwargs):
    
    # start session
    session = sessionmaker(bind=engine)()
    execution_date = kwargs['execution_date']

    try:
        logger.info("Executing stored procedure: CALL stg.agg_tripsdata()")
        session.execute(text('CALL "stg".agg_tripsdata();'))
        session.commit()
        logger.info("Stored procedure executed and transformation successful")
    except Exception as e:
        session.rollback()
        logger.error("Error executing stored procedure: %s", e)
        raise
    finally:
        logger.info(f'Pipeline for {execution_date} ran successfully')
        session.close()

default_args = {
    'owner': 'ridwanclouds',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Taxi_Trips_Pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for Taxi Trips data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# defining airflow tasks

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=fetch_data,
    op_kwargs={'execution_date': '{{ ds }}'},
    provide_context=True,
    dag=dag,
)

stage_data = PythonOperator(
    task_id='stage_data',
    python_callable=staging_data,
    provide_context=True,
    dag=dag,
)

execute_stored_procedure = PythonOperator(
    task_id='execute_stored_procedure',
    python_callable=transform_and_load,
    provide_context=True,
    dag=dag,
)

# Tasks flow
extract_data >> stage_data >> execute_stored_procedure
