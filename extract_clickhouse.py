import pandas as pd

# functions to fech data from source
def fetch_data(client, query):
    '''
    fetches query results from a clickhouse database and writes to a csv file

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
    df.to_csv('tripsdata.csv', index=False)

    print(f'{len(df)} rows successfully extracted from TaxiTrip Database')
