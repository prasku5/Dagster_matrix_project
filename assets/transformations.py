from dagster import asset, DailyPartitionsDefinition, WeeklyPartitionsDefinition, Output
import duckdb
import pandas as pd

daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")
weekly_partitions = WeeklyPartitionsDefinition(start_date="2023-01-01")

# Helper function to write data to DuckDB
def write_to_duckdb(table_name, df):
    con = duckdb.connect("my_database.duckdb")
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    con.close()

@asset(partitions_def=daily_partitions)
def transform_s3_files(context, s3_files) -> Output[dict]:
    partition_date = context.partition_key
    transformed_data = {}
    
    for key, df in s3_files.items(): 
        transformed_df = df  # Apply your transformation logic here
        table_name = key.split('/')[-1].replace('.csv', '')  # Generate table name from key
        write_to_duckdb(table_name, transformed_df)
        transformed_data[table_name] = transformed_df

    return Output(transformed_data, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions) # This ensures the date range is within the daily partitions
def transform_raw_table_1(context, raw_table_1) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    transformed_df = raw_table_1  # Apply your transformation logic here
    write_to_duckdb('transformed_table_1', transformed_df)
    return Output(transformed_df, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def transform_raw_table_2(context, raw_table_2) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    transformed_df = raw_table_2  # Apply your transformation logic here
    write_to_duckdb('transformed_table_2', transformed_df)
    return Output(transformed_df, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def transform_raw_table_3(context, raw_table_3) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    transformed_df = raw_table_3  # Apply your transformation logic here
    write_to_duckdb('transformed_table_3', transformed_df)
    return Output(transformed_df, metadata={"partition_date": partition_date})

@asset(partitions_def=weekly_partitions)
def transform_raw_weekly_table_1(context, raw_weekly_table_1) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    transformed_df = raw_weekly_table_1  # Apply your transformation logic here
    write_to_duckdb('transformed_weekly_table_1', transformed_df)
    return Output(transformed_df, metadata={"partition_date": partition_date})
