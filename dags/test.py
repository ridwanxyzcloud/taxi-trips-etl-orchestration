import sys
import os
from datetime import timedelta

# Add the project root to PYTHONPATH
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import logging
from db_utils import get_client
from extract_clickhouse import ini_fetch_data

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_data_from_clickhouse(**kwargs):
    client = get_client()
    query = ''' 
        SELECT pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount 
        FROM tripdata'''
    try:
        logger.info("Executing query: %s", query)
        ini_fetch_data(client=client, query=query)
        logger.info("Data fetched successfully")
    except Exception as e:
        logger.error("Error fetching data: %s", e)
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'Clickhouse_Fetch_Data_Pipeline',
    default_args=default_args,
    description='A pipeline to fetch data from Clickhouse',
    schedule_interval='@hourly',  # Changed to run every hour
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_from_clickhouse,
        provide_context=True,
    )

# Define task dependencies if there are multiple tasks
fetch_data_task
