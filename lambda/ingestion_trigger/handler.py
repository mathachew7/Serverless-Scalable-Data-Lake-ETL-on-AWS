import json
import os
import urllib.parse
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SUPPORTED_EXTENSIONS = {".csv", ".json", ".parquet", ".gz"}


def lambda_handler(event, context):
    records = event.get("Records", [])
    logger.info("Received %d S3 event record(s)", len(records))

    results = []
    for record in records:
        result = _process_record(record)
        results.append(result)

    return {"statusCode": 200, "body": json.dumps(results)}


def _process_record(record):
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    size = record["s3"]["object"].get("size", 0)

    logger.info("Processing s3://%s/%s (%d bytes)", bucket, key, size)

    if not _is_supported(key):
        logger.warning("Skipping unsupported file type: %s", key)
        return {"key": key, "status": "skipped", "reason": "unsupported_extension"}

    if size == 0:
        logger.warning("Skipping empty file: %s", key)
        return {"key": key, "status": "skipped", "reason": "empty_file"}

    file_format = _detect_format(key)
    run_id = _start_glue_job(bucket, key, file_format)

    logger.info("Started Glue job run %s for %s", run_id, key)
    return {"key": key, "status": "triggered", "glue_run_id": run_id, "format": file_format}


def _is_supported(key: str) -> bool:
    _, ext = os.path.splitext(key.lower())
    return ext in SUPPORTED_EXTENSIONS


def _detect_format(key: str) -> str:
    key_lower = key.lower()
    if key_lower.endswith(".csv"):
        return "csv"
    if key_lower.endswith(".json") or key_lower.endswith(".jsonl"):
        return "json"
    if key_lower.endswith(".parquet"):
        return "parquet"
    return "unknown"


def _start_glue_job(bucket: str, key: str, file_format: str) -> str:
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--INPUT_BUCKET": bucket,
            "--INPUT_KEY": key,
            "--FILE_FORMAT": file_format,
        },
    )
    return response["JobRunId"]
