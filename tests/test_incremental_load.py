import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from helpers import get_client, get_postgres_engine
from extract_clickhouse import fetch_data

client = get_client()
query = 'SELECT * FROM tripdata LIMIT 50'

# Execute the fetch_data with parameters client and query
fetch_data(client=client, query=query)

# execute the load_
