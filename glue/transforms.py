"""
Pure PySpark transform functions — no AWS Glue imports.
Importable and testable locally with a plain SparkSession.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

# Columns tried in order when looking for a timestamp to partition by
_TS_CANDIDATES = ["created_at", "timestamp", "event_time", "date", "updated_at"]


# ── Column normalization ──────────────────────────────────────────────────────

def clean_column_names(df: DataFrame) -> DataFrame:
    """Lowercase + snake_case every column name."""
    for col in df.columns:
        clean = (
            col.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )
        if clean != col:
            df = df.withColumnRenamed(col, clean)
    return df


# ── Row-level cleaning ────────────────────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


def drop_all_null_rows(df: DataFrame) -> DataFrame:
    return df.dropna(how="all")


# ── Partitioning ──────────────────────────────────────────────────────────────

def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add year / month / day columns for Hive-style partitioning.
    Uses the first recognised timestamp column; falls back to current_timestamp.
    """
    ts_col = next((c for c in _TS_CANDIDATES if c in df.columns), None)
    ts     = F.to_timestamp(ts_col) if ts_col else F.current_timestamp()
    return (
        df
        .withColumn("_ts",  ts)
        .withColumn("year",  F.year("_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("_ts").cast("string"), 2, "0"))
        .drop("_ts")
    )


# ── Schema enforcement ────────────────────────────────────────────────────────

def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Cast existing columns to expected types.
    Add missing nullable columns as null (so downstream queries don't break).
    """
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
        elif field.nullable:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df


# ── Data quality + quarantine ─────────────────────────────────────────────────

def split_quarantine(
    df: DataFrame,
    not_null_cols: list,
    positive_cols: list = None,
) -> tuple:
    """
    Split df into (clean_df, quarantine_df).

    A row is quarantined if:
      - Any column in not_null_cols is null
      - Any column in positive_cols is <= 0

    quarantine_df gets an extra '_quarantine_reason' column (first failing rule).
    """
    positive_cols = positive_cols or []

    # Build composite bad-row predicate
    is_bad = F.lit(False)
    for col in not_null_cols:
        if col in df.columns:
            is_bad = is_bad | F.col(col).isNull()
    for col in positive_cols:
        if col in df.columns:
            is_bad = is_bad | (F.col(col) <= 0)

    # Build first-failure reason string (last rule = lowest priority → build reversed)
    reason = F.lit("unknown")
    for col in reversed(positive_cols):
        if col in df.columns:
            reason = F.when(F.col(col) <= 0,      F.lit(f"non_positive:{col}")).otherwise(reason)
    for col in reversed(not_null_cols):
        if col in df.columns:
            reason = F.when(F.col(col).isNull(),   F.lit(f"null:{col}")).otherwise(reason)

    df_marked = df.withColumn(
        "_quarantine_reason",
        F.when(is_bad, reason).otherwise(F.lit(None)),
    )

    clean      = df_marked.filter(~is_bad).drop("_quarantine_reason")
    quarantine = df_marked.filter(is_bad)

    return clean, quarantine


def quality_metrics(raw: DataFrame, clean: DataFrame, quarantine: DataFrame) -> dict:
    """Return a metrics dict and warn if pass rate drops below 80 %."""
    raw_n   = raw.count()
    clean_n = clean.count()
    quar_n  = quarantine.count()
    rate    = round(clean_n / raw_n * 100, 2) if raw_n else 0.0

    metrics = {
        "raw_count":        raw_n,
        "clean_count":      clean_n,
        "quarantine_count": quar_n,
        "pass_rate_pct":    rate,
    }
    logger.info("Quality metrics: %s", metrics)
    if rate < 80:
        logger.warning("Pass rate %.1f%% is below 80%% — check quarantine path", rate)
    return metrics
