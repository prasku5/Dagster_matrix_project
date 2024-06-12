# assets/s3_assets.py

from dagster import asset, Output, DailyPartitionsDefinition
import pandas as pd
import boto3

# Define a partition for daily data fetching
daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")

# Helper function to fetch data from S3
def fetch_data_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = pd.read_csv(obj["Body"])
    return data

# Generic asset definition to handle multiple S3 files
@asset(partitions_def=daily_partitions)
def s3_files(context) -> Output[dict]:
    partition_date = context.partition_key  # Use partition key for S3 key
    file_keys = [
        
        f"path/to/s3/files/file_1_{partition_date}.csv",
        f"path/to/s3/files/file_2_{partition_date}.csv",
        f"path/to/s3/files/file_3_{partition_date}.csv"
    ]

    data_frames = {}

    for key in file_keys:
        data = fetch_data_from_s3("my-s3-bucket", key)
        data_frames[key] = data

    return Output(data_frames, metadata={"partition_date": partition_date})


# file_1_<date>.csv - Employee Personal Information

# employee_id,first_name,last_name,date_of_birth,gender
# 1,John,Doe,1985-05-15,M
# 2,Jane,Smith,1990-08-22,F
# 3,Jim,Brown,1982-11-30,M
# 4,Emily,Davis,1995-01-20,F

# file_2_<date>.csv - Employee Job Information

# employee_id,job_title,department,start_date,end_date
# 1,Software Engineer,IT,2010-06-01,
# 2,Data Scientist,Data,2012-09-15,
# 3,System Analyst,IT,2008-03-12,
# 4,Product Manager,Marketing,2018-07-23,

# file_3_<date>.csv - Employee Salary Information

# employee_id,salary,effective_date
# 1,90000,2022-01-01
# 2,120000,2022-01-01
# 3,75000,2022-01-01
# 4,110000,2022-01-01
