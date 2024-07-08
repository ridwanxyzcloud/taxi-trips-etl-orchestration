import pandas as pd 
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

def load_csv_to_postgres(csv_file_path, table_name, engine, schema):
    '''
    Loads dta from a csv file to a postgres DB table

    Parameters:
    - csv_file_path (str): Path to csv file 
    - table_name (str): a postgres DB table for staging
    - engine(sqlalchemy.engine): a SQL alchemy engine object
    - schema: postgres DB schema

    '''
    # reads csv to pandas and to sql
    df = pd.read_csv(csv_file_path)
    
    # the 'replace' will make sure no data is saved since it is a staging table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, schema=schema)

    print(f'{len(df)} rows loaded successfully to {table_name}')


# loading to snowflake
def load_csv_to_snowflake(csv_file_path, table_name, engine, schema):
    '''
    Loads dta from a csv file to a snowflake DB table

    Parameters:
    - csv_file_path (str): Path to csv file 
    - table_name (str): a snowflake DB table for staging
    - engine(sqlalchemy.engine): a SQL alchemy engine object
    - schema: snowflake DB schema

    '''
    # reads csv to pandas and to sql
    df = pd.read_csv(csv_file_path)
    
    # the 'replace' will make sure no data is saved since it is a staging table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, schema=schema)

    print(f'{len(df)} rows loaded successfully to {table_name}')


def execute_procedure(engine):
    # Execute stored procedure
    # transformation procedure for enterprise/ production use daily aggregate 
    session = sessionmaker(bind=engine)
    session = session()
    session.execute(('CALL TAXITRIPS.STG.AGG_TRIP_DATA()'))
    session.commit()

    print('Stored Procedure Excecuted Successfully')

