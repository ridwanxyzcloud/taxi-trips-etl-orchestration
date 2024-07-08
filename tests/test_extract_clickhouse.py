import sys
import os

# Add the project root to PYTHONPATH
sys.path.append('/opt/airflow')

from db_utils import get_client
from extract_clickhouse import ini_fetch_data, fetch_data

client = get_client()
query = ''' 
        SELECT pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount 
        FROM tripdata'''

# Execute the fetch data with parameters client and query
ini_fetch_data(client=client, query=query)
