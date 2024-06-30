from helpers import get_client
from extract_clickhouse import fetch_data

client = get_client()
query = 'select * from tripdata'

fetch_data(cleint=client, query=query)
