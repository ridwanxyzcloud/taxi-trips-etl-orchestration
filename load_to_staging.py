import pandas as pd 

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