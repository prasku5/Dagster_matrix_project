# resources/s3.py

from dagster import resource
import boto3

@resource
def s3_client(_):
    return boto3.client("s3")
