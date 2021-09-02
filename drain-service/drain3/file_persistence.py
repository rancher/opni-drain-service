"""
Adopted from https://github.com/IBM/Drain3
"""

# Standard Library
import logging
import os
import pathlib
import time

# Third Party
import boto3
import botocore
from botocore.client import Config
from drain3.persistence_handler import PersistenceHandler
from elasticsearch import Elasticsearch

ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_BUCKET = os.getenv("S3_BUCKET", "opni-drain-model")


class FilePersistence(PersistenceHandler):
    def __init__(self, file_path):
        self.file_path = file_path
        self.connect_to_s3()
        self.last_s3_save_ts = -1
        self.es = Elasticsearch(
            [ES_ENDPOINT],
            port=9200,
            http_auth=(ES_USERNAME, ES_PASSWORD),
            http_compress=True,
            max_retries=10,
            retry_on_status={100, 400, 503},
            retry_on_timeout=True,
            timeout=20,
            use_ssl=True,
            verify_certs=False,
            sniff_on_start=False,
            # refresh nodes after a node fails to respond
            sniff_on_connection_fail=True,
            # and also every 60 seconds
            sniffer_timeout=60,
            sniff_timeout=10,
        )

    def connect_to_s3(self):
        self.s3_client = None
        try:
            self.s3_client = boto3.resource(
                "s3",
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                config=Config(signature_version="s3v4"),
            )
            logging.info("Connected to S3 client")
        except Exception as e:
            logging.error("Unable to connect to S3 client right now.")

        exists = True
        try:
            self.s3_client.meta.client.head_bucket(Bucket=S3_BUCKET)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                exists = False
        if exists:
            logging.info(f"{S3_BUCKET} bucket exists")
        else:
            logging.info(f"{S3_BUCKET} bucket does not exist so creating it now")
            self.s3_client.create_bucket(Bucket=S3_BUCKET)

    def save_state(self, state, num_drain_clusters):
        pathlib.Path(self.file_path).write_bytes(state)
        if time.time() - self.last_s3_save_ts > 60:
            try:
                self.s3_client.meta.client.upload_file(
                    self.file_path, S3_BUCKET, self.file_path
                )
                logging.info("Saved DRAIN model into S3")
                self.last_s3_save_ts = time.time()
            except Exception as e:
                logging.error("Unable to save DRAIN model into S3")

        drain_status_doc = {
            "num_log_clusters": num_drain_clusters,
            "update_type": "drain_persist",
            "timestamp": int(time.time() * 1000),
        }
        try:
            res = self.es.index(index="opni-drain-model-status", body=drain_status_doc)
        except Exception as e:
            logging.error("Error when indexing status to opni-drain-model-status")

    def load_state(self):
        try:
            self.s3_client.meta.client.download_file(
                S3_BUCKET, self.file_path, self.file_path
            )
            logging.info("Downloaded DRAIN model file from S3")
        except Exception as e:
            logging.error("Cannot currently obtain DRAIN model file")
        if not os.path.exists(self.file_path):
            return None

        return pathlib.Path(self.file_path).read_bytes()
