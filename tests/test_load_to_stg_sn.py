import sys
import os

# Adding the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_utils import get_snowflake_engine
from load_data import load_csv_to_snowflake

# define paarameters
engine = get_snowflake_engine()
schema = 'STG'
table_name = 'src_tripsdata'
csv_file_path = './raw_data/tripsdata.csv'

# execute the load_to_staging function
load_csv_to_snowflake(csv_file_path, table_name, engine, schema)