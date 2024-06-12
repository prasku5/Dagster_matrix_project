from dagster import asset, Output, DailyPartitionsDefinition, WeeklyPartitionsDefinition
import pandas as pd
import pyodbc

# Define daily partitions
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")

# Define weekly partitions
weekly_partitions = WeeklyPartitionsDefinition(start_date="2023-01-01")

# Helper function to connect to SQL Server
def fetch_data_from_sql_server(query, params):
    conn = pyodbc.connect("DRIVER={SQL Server};SERVER=server_name;DATABASE=db_name;UID=user;PWD=password")
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

@asset(partitions_def=daily_partitions)
def raw_table_1(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM table_1 WHERE DATE = ?"
    partition_date = context.partition_key  # Use partition key for query parameter
    data = fetch_data_from_sql_server(query, (partition_date,)) # This creates a tuple with one element.
    return Output(data, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def raw_table_2(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM table_2 WHERE DATE = ?"
    partition_date = context.partition_key
    data = fetch_data_from_sql_server(query, (partition_date,))
    return Output(data, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def raw_table_3(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM table_3 WHERE DATE = ?"
    partition_date = context.partition_key
    data = fetch_data_from_sql_server(query, (partition_date,))
    return Output(data, metadata={"partition_date": partition_date})

@asset(partitions_def=weekly_partitions)
def raw_weekly_table_1(context) -> Output[pd.DataFrame]:
    query = "SELECT * FROM weekly_table_1 WHERE DATE = ?"
    partition_date = context.partition_key  # Use partition key for query parameter
    data = fetch_data_from_sql_server(query, (partition_date,))
    return Output(data, metadata={"partition_date": partition_date})
