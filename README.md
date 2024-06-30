# Taxi Trips ETL Orchestration 

- This project used modular coding to systematically define an ETL job with seperate and well defined logic
- this architecture extracts data from the source (clickhouse), writes to a csv file, then load to a staging environment. from a staging environment
- The data in the staging environment is aggregated and transformed to an enterprise data warehouse where the data is ready to be used for all necessary analytics 

- github.demo.trial.altinity.cloud 

# Steps

### 1. Module `db_utils.py` 

In this module, three functions are defined. 
- i. The `get_client()` function that connects to clickhouse which is the data source and return a database client object
- ii. The `get_postgres_engine()` function that constructs a SQLalchemy engine object. It creates connection to posgresSQL database where the extracted data is staged(loaded).
- iii. The `get_postgres_engine2()` is a backup postgres database connection for macOS users when they encounter errror creating sqlalchemy.engine due to how postgreSQL is configured to use  Unix domain socket TCP/IP.

### 2. Module `extract_clickhouse.py`
This module house a function `fetch_data(client, query)` that defines the logic that extracts data from the source. This function takes two parameters. The client (connection to source database) and query (sql statement defining the data needed from the source database)

The 'result' is output of the query, and it is returned in rows and columns. It uses 'result_rows' and 'column_names' which are both method on the 'result' object  to return rows and columns of data extracted.

The 'result' is written to a csv file waiting to be loaded to a staging area.

### 3. Module `load_to_staging,py`

This module contains a function that defines the logic for loading the extracted data incrementally into a staging table on a postgres database.

### 4. `Test` directory
The test directory conatians unit test for the modules of the project. 

### 5. The `main.py`: The Pipeline
The `main.py` defines house the logic of the pipeline.
- The pipeline fetches data from source database(clickhouse) as a query result and save to csv from a dataframe
- Loads the data from a csv file directly to a staging table 'tripdata' on postgres using sqlachemy.engine
- Performs an incremental loading to the staging table 

# Production part of the project
