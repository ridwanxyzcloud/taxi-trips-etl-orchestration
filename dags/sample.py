from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'snowflake_etl',
    default_args=default_args,
    description='A simple ETL DAG using Snowflake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 7),
    catchup=False,
)

def extract_data():
    # Extract data from Snowflake
    hook = SnowflakeHook(snowflake_conn_id='your_snowflake_conn_id')
    query = "SELECT * FROM your_source_table"
    df = hook.get_pandas_df(sql=query)
    df.to_csv('/tmp/extracted_data.csv', index=False)

def transform_data():
    # Transform the data using pandas
    df = pd.read_csv('/tmp/extracted_data.csv')
    # Example transformation: add a new column
    df['new_column'] = df['existing_column'] * 2
    df.to_csv('/tmp/transformed_data.csv', index=False)

def load_data():
    # Load the transformed data back into Snowflake
    hook = SnowflakeHook(snowflake_conn_id='your_snowflake_conn_id')
    df = pd.read_csv('/tmp/transformed_data.csv')
    hook.write_pandas(df, 'your_target_table', chunk_size=1000)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> load_task
