import sqlalchemy
from sqlalchemy import create_engine
import clickhouse_connect
from dotenv import load_dotenv
import os 
from snowflake.sqlalchemy import URL
import snowflake.connector

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
    user = os.getenv('pg_user')
    password = os.getenv('pg_password')
    host = os.getenv('pg_host')
    port = os.getenv('pg_port')
    dbname = os.getenv('pg_dbname')
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(connection_string)
    return engine


## Additional postgres engine for macOS 

def get_postgres_engine2():
    # Use URL object to create connection string
    connection_url = URL(
        drivername="postgresql+psycopg2",
        username=os.getenv('pg_user'),
        password=os.getenv('pg_password'),
        host=os.getenv('pg_host'),
        port=os.getenv('pg_port'),
        database=os.getenv('pg_dbname')
    )
    # Create engine
    engine = create_engine(connection_url)
    return engine

# snowflake connection 

def get_snowflake_engine():

    '''
    constructs a snowflake engine object for snowflake DB from .env file

    parameter: None

    Returns: 
     - snowflake-connector engine (sqlalchemy.Engine)
    '''

    # create engine for snowflake
    try:
        # Create Snowflake URL
        snowflake_url = URL(
            user=os.getenv('sn_user'),
            password=os.getenv('sn_password'),
            account=os.getenv('sn_account_identifier'),
            database=os.getenv('sn_database'),
            schema=os.getenv('sn_schema'),
            warehouse=os.getenv('sn_warehouse'),
            role=os.getenv('sn_role')
        )

        # Create SQLAlchemy engine and return it
        engine = create_engine(snowflake_url)
        return engine

    except Exception as e:
        print(f"Error creating Snowflake engine: {e}")
        return None