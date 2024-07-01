from db_utils import get_client, get_postgres_engine, get_postgres_engine2
from extract_clickhouse import fetch_data
from load_to_staging import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# extract parameters
client = get_client()
query = '''

        SELECT * FROM tripdata
        where year(pickup_date) = 2016 and month(pickup_date) = 1 and dayofmonth(pickup_date) = 1

        '''
# load parameters
engine = get_postgres_engine2()
schema = 'stg'
table_name = 'tripsdata'
csv_file_path = '/Users/villy/Documents/GitHub/taxi-trips-etl-orchestration/tripsdata.csv'

# Pipeline logic for extraction and Staging
def main():
    '''
    Main function to run the data pipeline modules
    1. Fetches data from source database(clickhouse) as a query result and save to csv from a dataframe
    2. Loads the data from a csv file directly to a staging table 'tripdata' on postgres using sqlachemy.engine
    3. Performs an incremental loading to the staging table 

    Parameters: None

    Return : None
    '''
    # extract data to a csv
    fetch_data(client=client, query=query)

    # Load data to staging table
    load_csv_to_postgres(csv_file_path, table_name, engine, schema)
    
    # Execute stored procedure
    # transformation procedure for enterprise/ production use daily aggregate 
    session = sessionmaker(bind=engine)
    session = session()
    session.execute(text('CALL "stg".agg_tripsdata();'))
    session.commit()

    print('stored procedure executed and transfomation successfull')

    print('Pipeline executed successfully')

if __name__ == '__main__':
    main()