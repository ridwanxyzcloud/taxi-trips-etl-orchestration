# Taxi Trips ETL Orchestration 

- This project used modular coding to systematically define an ETL job with seperate and well defined logic
- this architecture extracts data from the source (clickhouse), writes to a csv file, then load to a staging environment. from a staging environment
- The data in the staging environment is aggregated and transformed to an enterprise data warehouse where the data is ready to be used for all necessary analytics 

- github.demo.trial.altinity.cloud 

# Steps

### 1. Module `helpers.py` 

In this module, two functions are defined. 
- i. The `get_client()` function that connects to clickhouse which is the data source and return a database client object
- ii. The `get_postgres_engine()` function that constructs a SQLalchemy engine object. It creates connection to posgresSQL database where the extracted data is staged(loaded).

### 2. Module `extract_clickhouse.py`
This module house a function `fetch_data(client, query)` that defines the logic that extracts data from the source. This function takes two parameters. The client (connection to source database) and query (sql statement defining the data needed from the source database)

The 'result' is output of the query, and it is returned in rows and columns. It uses 'result_rows' and 'column_names' which are both method on the 'result' object  to return rows and columns of data extracted.

The 'result' is written to a csv file waiting to be loaded to a staging area.

### 3. Module `load_to_staging,py`

This module contains a function that defines the logic for loading the extracted data incrementally into a staging table on a postgres database.

The 

