import pytest
from moto import mock_s3
from botocore.client import Config
import boto3
from drain_service.drain3 import file_persistence
import os

@mock_s3
def test_class_FilePersistence():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=os.environ["S3_BUCKET"])
    fp = file_persistence.FilePersistence("test_file")
