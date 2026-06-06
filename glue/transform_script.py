"""
AWS Glue ETL entry point.
All transform logic lives in transforms.py (testable locally).
"""
import json
import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from schemas import SCHEMA_MAP
from transforms import (
    add_partition_columns,
    clean_column_names,
    deduplicate,
    drop_all_null_rows,
    enforce_schema,
    quality_metrics,
    split_quarantine,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Bootstrap ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "DATABASE_NAME",
    "ENVIRONMENT",
    "--INPUT_BUCKET",
    "--INPUT_KEY",
    "--FILE_FORMAT",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH      = f"s3://{args['--INPUT_BUCKET']}/{args['--INPUT_KEY']}"
PROCESSED_PATH  = f"s3://{args['PROCESSED_BUCKET']}/data/"
QUARANTINE_PATH = f"s3://{args['PROCESSED_BUCKET']}/quarantine/"
FILE_FORMAT     = args["--FILE_FORMAT"]

logger.info("ETL start | input=%s | format=%s", INPUT_PATH, FILE_FORMAT)


# ── Extract ───────────────────────────────────────────────────────────────────
def _read_raw(path: str, fmt: str):
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    if fmt == "json":
        return spark.read.option("multiline", "true").json(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported format: {fmt}")


df_raw = _read_raw(INPUT_PATH, FILE_FORMAT)
logger.info("Raw record count: %d", df_raw.count())


# ── Transform ─────────────────────────────────────────────────────────────────
schema, not_null_cols, positive_cols = SCHEMA_MAP.get(FILE_FORMAT, (None, [], []))

df = (
    df_raw
    .transform(clean_column_names)
    .transform(deduplicate)
    .transform(drop_all_null_rows)
)

if schema:
    df = enforce_schema(df, schema)

df = add_partition_columns(df)

clean_df, quarantine_df = split_quarantine(df, not_null_cols, positive_cols)

metrics = quality_metrics(df_raw, clean_df, quarantine_df)
logger.info("Metrics: %s", json.dumps(metrics))


# ── Load ──────────────────────────────────────────────────────────────────────
(
    clean_df
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(PROCESSED_PATH)
)
logger.info("Clean data written to %s", PROCESSED_PATH)

if quarantine_df.count() > 0:
    (
        quarantine_df
        .write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(f"{QUARANTINE_PATH}source={FILE_FORMAT}/")
    )
    logger.warning(
        "%d quarantine rows written to %ssource=%s/",
        quarantine_df.count(), QUARANTINE_PATH, FILE_FORMAT,
    )

logger.info("ETL complete")
job.commit()
