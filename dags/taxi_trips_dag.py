import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# imoport fucntion from other modules in the parent directory (project repository)
from db_utils import get_client, get_postgres_engine, get_postgres_engine2
from extract_clickhouse import fetch_data
from load_to_staging import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# import airflow functions
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

# postgres engine
engine = get_postgres_engine2()
# execution_date defination 
execution_date = context['execution_date']

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pipeline logic for extraction and Staging
def fetch_data():

    client = get_client()
    query = f'''
                SELECT * FROM tripdata
                WHERE pickup_date = '{execution_date.strftime("%Y-%m-%d")}'
            '''
    try:
        logger.info("Executing query: %s", query)
        fetch_data(client=client, query=query)
        logger.info("Data fetched successfully for date: %s", execution_date)
    except Exception as e:
        logger.error("Error fetching data: %s", e)
        raise

# staging fetched data
def staging_data():

    schema = 'stg'
    table_name = 'tripsdata'
    csv_file_path = '/Users/villy/Documents/GitHub/taxi-trips-etl-orchestration/tripsdata.csv'

    try:
        logger.info("Loading data from %s into %s.%s", csv_file_path, schema, table_name)
        load_csv_to_postgres(csv_file_path, table_name, engine, schema)
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

def transform_and_load():
    
    # start session
    session = sessionmaker(bind=engine)
    session = session()

    try:
        logger.info("Executing stored procedure: CALL stg.agg_tripsdata()")
        session.execute(text('CALL "stg".agg_tripsdata();'))
        session.commit()
        logger.info("stored procedure executed and transfomation successfull")
    except Exception as e:
        session.rollback()
        logger.error("Error executing stored procedure: %s", e)
        raise
    finally:
        print(f'Pipeline for {execution_date} ran successfully')
        session.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
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
    op_args=['{{ ds }}'],
    dag=dag,
)

stage_data = PythonOperator(
    task_id='stage_data',
    python_callable=staging_data,
    dag=dag,
)

execute_stored_procedure = PythonOperator(
    task_id='execute_stored_procedure',
    python_callable=transform_and_load,
    dag=dag,
)

# Tasks flow
extract_data >> stage_data >> execute_stored_procedure