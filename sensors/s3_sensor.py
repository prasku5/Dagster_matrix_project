# sensors/s3_sensor.py

from dagster import sensor, RunRequest
import boto3
from datetime import timezone

@sensor(job_name="daily_job", minimum_interval_seconds=3600)  # Poll every hour
def s3_sensor(context):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket="my-s3-bucket", Prefix="path/to/s3/files/")
    last_run_time = context.last_completion_time

    for obj in response.get("Contents", []):
        # Check if the file is new
        last_modified = obj["LastModified"]
        if last_run_time is None or last_modified.replace(tzinfo=timezone.utc) > last_run_time:
            yield RunRequest(run_key=obj["Key"], run_config={})
    