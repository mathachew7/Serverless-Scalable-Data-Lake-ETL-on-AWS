import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import logging

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

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

INPUT_PATH = f"s3://{args['--INPUT_BUCKET']}/{args['--INPUT_KEY']}"
OUTPUT_PATH = f"s3://{args['PROCESSED_BUCKET']}/data/"
FILE_FORMAT = args["--FILE_FORMAT"]

logger.info("Starting ETL | input=%s | format=%s | output=%s", INPUT_PATH, FILE_FORMAT, OUTPUT_PATH)


# ── Extract ───────────────────────────────────────────────────────────────────
def read_raw(path: str, fmt: str):
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    if fmt == "json":
        return spark.read.option("multiline", "true").json(path)
    if fmt == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported format: {fmt}")


df_raw = read_raw(INPUT_PATH, FILE_FORMAT)
logger.info("Raw record count: %d", df_raw.count())


# ── Transform ─────────────────────────────────────────────────────────────────
def clean_column_names(df):
    """Lowercase and snake_case all column names."""
    renamed = {c: c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns}
    for old, new in renamed.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def add_partition_columns(df):
    """Add year/month/day columns for Hive-style partitioning."""
    if "created_at" in df.columns:
        ts_col = F.to_timestamp("created_at")
    elif "timestamp" in df.columns:
        ts_col = F.to_timestamp("timestamp")
    elif "date" in df.columns:
        ts_col = F.to_timestamp("date")
    else:
        ts_col = F.current_timestamp()

    return (
        df
        .withColumn("_ts", ts_col)
        .withColumn("year",  F.year("_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("_ts").cast("string"), 2, "0"))
        .drop("_ts")
    )


def deduplicate(df):
    return df.dropDuplicates()


def drop_nulls_in_key_cols(df):
    """Drop rows where ALL columns are null."""
    return df.dropna(how="all")


df_clean = (
    df_raw
    .transform(clean_column_names)
    .transform(deduplicate)
    .transform(drop_nulls_in_key_cols)
    .transform(add_partition_columns)
)

logger.info("Clean record count: %d", df_clean.count())


# ── Load ──────────────────────────────────────────────────────────────────────
(
    df_clean
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(OUTPUT_PATH)
)

logger.info("ETL complete. Written to %s", OUTPUT_PATH)
job.commit()
