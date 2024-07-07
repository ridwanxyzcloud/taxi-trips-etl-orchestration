import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_utils import get_postgres_engine, get_snowflake_connection
from load_to_staging import load_csv_to_postgres

if __name__ == "__main__":
    csv_file_path = r'C:\Users\villy\Documents\GitHub\taxi-trips-etl-orchestration\tripsdata.csv' 
    table_name = 'tripsdata'
    schema = 'STG' 

    engine = get_snowflake_connection()
    load_csv_to_postgres(csv_file_path, table_name, engine, schema)
