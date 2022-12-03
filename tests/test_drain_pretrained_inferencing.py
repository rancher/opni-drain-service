from drain_service import drain_pretrained_inferencing as dp
import pytest
from moto import mock_s3
from botocore.client import Config
import boto3

# @mock_s3
# def test_load_pretrain_model():
#     s3_client = boto3.resource(
#         "s3",
#         config=Config(signature_version="s3v4"),
#     )
#     p, c = dp.load_pretrain_model()
#     assert p is not None