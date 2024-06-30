import sqlalchemy
from sqlalchemy import create_engine
import clickhouse-connect
from dotenv import load_dotenv
import os 

load_dotenv(override=True)

def get_client():
    '''
    connects to a clickhouse database using paramters from .env file

    parameter: None

    Returns: 
     - Clickhouse_connect.Client: A database client object

    '''
    ## getting credentials
    host = os.getenv('ch_host')
    port = os.getenv('ch_port')
    user = os.getenv('ch_user')
    password = os.getenv('ch_password')

    ## connect to database
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=True)

    return client 

def get_postgres_engine():
    '''
    constructs a SQLalchemy engine object for postgres DB from .env file

    parameter: None

    Returns: 
     - sqlalchemy engine (sqlalchemy.engine.Engine)

    '''
    engine = create_engine()
