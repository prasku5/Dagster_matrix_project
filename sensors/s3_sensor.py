# sensors/s3_sensor.py

from dagster import sensor, RunRequest
import boto3

@sensor(job="daily_job", minimum_interval_seconds=3600)  # Poll every hour
def s3_sensor(context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket="my-s3-bucket", Prefix="path/to/s3/files/")
    for obj in response.get("Contents", []):
        if is_new_file(obj):
            yield RunRequest(run_key=obj["Key"], run_config={})
