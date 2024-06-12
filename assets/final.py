from dagster import asset, DailyPartitionsDefinition, WeeklyPartitionsDefinition, Output
import duckdb
import pandas as pd

daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")
weekly_partitions = WeeklyPartitionsDefinition(start_date="2023-01-01")

# Helper function to read data from DuckDB
def read_from_duckdb(table_name):
    con = duckdb.connect("my_database.duckdb")
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
    con.close()
    return df

# Helper function to write data to DuckDB (overwrite for SCD Type 1)
def write_to_duckdb(table_name, df):
    con = duckdb.connect("my_database.duckdb")
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.close()

@asset(partitions_def=daily_partitions)
def scd_type_1_s3_files(context, transform_s3_files) -> Output[dict]:
    partition_date = context.partition_key
    updated_data = {}

    for table_name, new_data in transform_s3_files.items():
        # Read existing data
        existing_data = read_from_duckdb(table_name)

        # Apply SCD Type 1 logic (overwrite existing data with new data)
        updated_table_data = new_data

        # Write updated data back to DuckDB
        write_to_duckdb(table_name, updated_table_data)
        updated_data[table_name] = updated_table_data

    return Output(updated_data, metadata={"partition_date": partition_date})


@asset(partitions_def=daily_partitions)
def scd_type_1_table_1(context, transform_raw_table_1) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    new_data = transform_raw_table_1

    # Read existing data
    existing_data = read_from_duckdb('scd_type_1_table_1')

    # Apply SCD Type 1 logic (overwrite existing data with new data)
    updated_data = new_data

    # Write updated data back to DuckDB
    write_to_duckdb('scd_type_1_table_1', updated_data)

    return Output(updated_data, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def scd_type_1_table_2(context, transform_raw_table_2) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    new_data = transform_raw_table_2

    # Read existing data
    existing_data = read_from_duckdb('scd_type_1_table_2')

    # Apply SCD Type 1 logic (overwrite existing data with new data)
    updated_data = new_data

    # Write updated data back to DuckDB
    write_to_duckdb('scd_type_1_table_2', updated_data)

    return Output(updated_data, metadata={"partition_date": partition_date})

@asset(partitions_def=daily_partitions)
def scd_type_1_table_3(context, transform_raw_table_3) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    new_data = transform_raw_table_3

    # Read existing data
    existing_data = read_from_duckdb('scd_type_1_table_3')

    # Apply SCD Type 1 logic (overwrite existing data with new data)
    updated_data = new_data

    # Write updated data back to DuckDB
    write_to_duckdb('scd_type_1_table_3', updated_data)

    return Output(updated_data, metadata={"partition_date": partition_date})

@asset(partitions_def=weekly_partitions)
def scd_type_1_weekly_table_1(context, transform_raw_weekly_table_1) -> Output[pd.DataFrame]:
    partition_date = context.partition_key
    new_data = transform_raw_weekly_table_1

    # Read existing data
    existing_data = read_from_duckdb('scd_type_1_weekly_table_1')

    # Apply SCD Type 1 logic (overwrite existing data with new data)
    updated_data = new_data

    # Write updated data back to DuckDB
    write_to_duckdb('scd_type_1_weekly_table_1', updated_data)

    return Output(updated_data, metadata={"partition_date": partition_date})