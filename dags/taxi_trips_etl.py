import sys
import os
from datetime import timedelta

# Add the project root to PYTHONPATH
sys.path.append('/opt/airflow')

# importing libraries
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Load libraries from modules in project base
from db_utils import get_client, get_snowflake_engine
from extract_clickhouse import fetch_data
from load_data import load_csv_to_snowflake, execute_procedure

# getting client and engine
engine = get_snowflake_engine()
client = get_client()

## default arguement 
default_args = {
    'owner': 'ridwanclouds',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'retry_delay': timedelta(minutes=40),
}

with DAG(
    'Taxi_Trips_ETL',
    default_args=default_args,
    description='Taxi Trips ETL pipeline for tripsdata',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # task 1
    start_task = DummyOperator(
        task_id = 'Start_Pipeline'
    )

    # task 2 
    extract_task = PythonOperator(
        task_id = 'extract',
        python_callable=fetch_data,
        op_kwargs = {'client': client, 'engine': engine}
    )

    # staging load task
    ## task 3

    staging_load_task = PythonOperator(
        task_id = 'stg_load',
        python_callable=load_csv_to_snowflake,
        op_kwargs = {'table_name': 'tripsdata', 'engine': engine, 'schema': 'STG'}      
    )

    # Loading to production
    ## task 4

    prodload_task = PythonOperator(
        task_id = 'edw_load',
        python_callable = execute_procedure,
        op_kwargs = {'engine': engine}
    )


    ## end task
    end_task = DummyOperator(
        task_id = 'end_pipeline'
    )

# setting dependencies
start_task >> extract_task >> staging_load_task >> prodload_task >> end_task
