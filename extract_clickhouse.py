import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker

# functions to fetch bulk data from source
def ini_fetch_data(client, query):
    '''
    fetches query results from a clickhouse database and writes to a csv file

    This function is for initial bulk data extraction 
    parameters:
    - client(clickhouse_connect.client)
    - query(SQL select query )

    Returns: None
    '''
    # execute the query
    result = client.query(query)
    rows = result.result_rows
    cols = result.column_names

    # close the connection 
    client.close()

    # write 'result' to pandas .csv file
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(r'./raw_data/tripsdata.csv', index=False)

    print(f'{len(df)} rows successfully extracted from clickhouse Database')

# Function for incremental fetching of data from source 
def fetch_data(client, engine):
    '''
    fetches query results from a clickhouse database and writes to a csv file

    This function is for incrementally fetching data from the source
    - Uses 'session' to connect and query 'snowflake DB'
    - Gets the date of the last 'pickup_date' : 'max_date'
    - query the data source based on an incremental data : toDate('(max_date)') + 1
    parameters:
    - client(clickhouse_connect.client)
    - query(SQL select query )

    Returns: None
    '''    
    session = sessionmaker(bind=engine)
    session = session()
    result = session.execute('select max(pickup_date) from "STG".src_tripsdata')
    max_date = result.fetchone()[0]
    session.close()

    ## getting the new date
    new_date = (datetime.strptime(max_date, '%Y-%m-%d') + timedelta(days=1)).date()

    query = f'''
        select pickup_date, vendor_id, passenger_count, trip_distance, payment_type, fare_amount, tip_amount
        from tripdata
        where pickup_date = toDate('{max_date}') + 1
        '''
    
    # execute the query
    result = client.query(query)
    rows = result.result_rows
    cols = result.column_names
    
    # close the client    
    client.close()

    # write 'result' to pandas .csv file
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv('./raw_data/tripsdata.csv', index=False)

    print(f'{len(df)} rows successfully extracted for {new_date}')
