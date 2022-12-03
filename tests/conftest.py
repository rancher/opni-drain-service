import os
# os.environ["S3_ENDPOINT"] = None
# os.environ["S3_ACCESS_KEY"] = None
# os.environ["S3_SECRET_KEY"] = None
os.environ["S3_BUCKET"] = "opni-drain-model"
os.environ["NATS_SERVER_URL"] = ""
os.environ["NATS_USERNAME"] = ""
os.environ["NATS_PASSWORD"] = ""
os.chdir("./drain_service")