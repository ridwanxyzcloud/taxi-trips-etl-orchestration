import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_utils import get_client
from extract_clickhouse import fetch_data

client = get_client()
query = 'SELECT * FROM tripdata'

# Execute the fetch data with parameters client and query
fetch_data(client=client, query=query)
