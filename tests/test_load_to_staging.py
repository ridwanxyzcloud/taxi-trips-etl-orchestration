import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_utils import get_postgres_engine, get_postgres_engine2
from load_to_staging import load_csv_to_postgres

# define paarameters
engine = get_postgres_engine2()
schema = 'stg'
table_name = 'tripsdata'
csv_file_path = '/Users/villy/Documents/GitHub/taxi-trips-etl-orchestration/tripsdata.csv'

# execute the load_to_staging function
load_csv_to_postgres(csv_file_path, table_name, engine, schema)