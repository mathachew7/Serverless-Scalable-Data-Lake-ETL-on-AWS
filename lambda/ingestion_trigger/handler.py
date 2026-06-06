import json
import os
import urllib.parse
from datetime import datetime, timezone
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3   = boto3.client("s3")
glue = boto3.client("glue")

GLUE_JOB_NAME       = os.environ["GLUE_JOB_NAME"]
RAW_BUCKET          = os.environ["RAW_BUCKET"]
SUPPORTED_EXTENSIONS = {".csv", ".json", ".jsonl", ".parquet", ".gz"}


def lambda_handler(event, context):
    records = event.get("Records", [])
    logger.info("Received %d S3 event record(s)", len(records))
    results = [_process_record(r) for r in records]
    return {"statusCode": 200, "body": json.dumps(results)}


def _process_record(record):
    bucket = record["s3"]["bucket"]["name"]
    key    = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    size   = record["s3"]["object"].get("size", 0)

    logger.info("Processing s3://%s/%s (%d bytes)", bucket, key, size)

    if not _is_supported(key):
        logger.warning("Skipping unsupported extension: %s", key)
        return {"key": key, "status": "skipped", "reason": "unsupported_extension"}

    if size == 0:
        logger.warning("Skipping empty file: %s", key)
        return {"key": key, "status": "skipped", "reason": "empty_file"}

    fmt           = _detect_format(key)
    organized_key = _organize_key(key, fmt)

    # Copy from uploads/ into the organized raw zone: raw/{fmt}/year=/month=/day=/
    s3.copy_object(
        CopySource={"Bucket": bucket, "Key": key},
        Bucket=RAW_BUCKET,
        Key=organized_key,
    )
    logger.info("Organized copy: s3://%s/%s", RAW_BUCKET, organized_key)

    run_id = _start_glue_job(RAW_BUCKET, organized_key, fmt)
    logger.info("Glue run started: %s", run_id)

    return {
        "original_key":  key,
        "organized_key": organized_key,
        "status":        "triggered",
        "glue_run_id":   run_id,
        "format":        fmt,
    }


def _is_supported(key: str) -> bool:
    _, ext = os.path.splitext(key.lower())
    return ext in SUPPORTED_EXTENSIONS


def _detect_format(key: str) -> str:
    k = key.lower()
    if k.endswith(".csv"):                return "csv"
    if k.endswith((".json", ".jsonl")):   return "json"
    if k.endswith(".parquet"):            return "parquet"
    return "unknown"


def _organize_key(original_key: str, fmt: str) -> str:
    """
    Moves uploads/{filename} → raw/{fmt}/year=YYYY/month=MM/day=DD/{filename}
    Hive-style partitioning so Glue/Athena can partition-prune from the raw zone too.
    """
    now      = datetime.now(timezone.utc)
    filename = os.path.basename(original_key)
    return (
        f"raw/{fmt}/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{filename}"
    )


def _start_glue_job(bucket: str, key: str, fmt: str) -> str:
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--INPUT_BUCKET": bucket,
            "--INPUT_KEY":    key,
            "--FILE_FORMAT":  fmt,
        },
    )
    return response["JobRunId"]
