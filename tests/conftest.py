import os

# Set before boto3 is imported anywhere in the test session
os.environ.setdefault("AWS_DEFAULT_REGION",    "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("GLUE_JOB_NAME",         "test-etl-job")
os.environ.setdefault("RAW_BUCKET",            "test-raw-bucket")
os.environ.setdefault("PROCESSED_BUCKET",      "test-processed-bucket")
os.environ.setdefault("ENVIRONMENT",           "test")
