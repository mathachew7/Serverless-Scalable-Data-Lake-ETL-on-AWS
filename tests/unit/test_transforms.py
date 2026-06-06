import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../glue"))

from transforms import (
    clean_column_names,
    deduplicate,
    drop_all_null_rows,
    add_partition_columns,
    enforce_schema,
    split_quarantine,
    quality_metrics,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transforms")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── clean_column_names ────────────────────────────────────────────────────────

def test_clean_column_names_spaces(spark):
    df = spark.createDataFrame([("x",)], ["Column Name"])
    assert "column_name" in clean_column_names(df).columns

def test_clean_column_names_hyphens(spark):
    df = spark.createDataFrame([("x",)], ["some-col"])
    assert "some_col" in clean_column_names(df).columns

def test_clean_column_names_uppercase(spark):
    df = spark.createDataFrame([("x",)], ["OrderID"])
    assert "orderid" in clean_column_names(df).columns

def test_clean_column_names_already_clean(spark):
    df = spark.createDataFrame([("x",)], ["order_id"])
    assert "order_id" in clean_column_names(df).columns


# ── deduplicate ───────────────────────────────────────────────────────────────

def test_deduplicate_removes_exact_dupes(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], ["v"])
    assert deduplicate(df).count() == 2

def test_deduplicate_keeps_unique(spark):
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["v"])
    assert deduplicate(df).count() == 3


# ── drop_all_null_rows ────────────────────────────────────────────────────────

def test_drop_all_null_rows(spark):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", StringType(), True),
    ])
    data = [("x", "y"), (None, None), ("z", None)]
    df = spark.createDataFrame(data, schema)
    assert drop_all_null_rows(df).count() == 2


# ── add_partition_columns ─────────────────────────────────────────────────────

def test_partition_from_created_at(spark):
    df = spark.createDataFrame([("2024-03-15 10:00:00",)], ["created_at"])
    result = add_partition_columns(df).first()
    assert result["year"]  == "2024"
    assert result["month"] == "03"
    assert result["day"]   == "15"

def test_partition_from_timestamp_col(spark):
    df = spark.createDataFrame([("2024-07-04 00:00:00",)], ["timestamp"])
    result = add_partition_columns(df).first()
    assert result["year"]  == "2024"
    assert result["month"] == "07"
    assert result["day"]   == "04"

def test_partition_fallback_adds_columns(spark):
    df = spark.createDataFrame([("x",)], ["some_col"])
    result = add_partition_columns(df)
    assert "year"  in result.columns
    assert "month" in result.columns
    assert "day"   in result.columns

def test_partition_original_ts_col_preserved(spark):
    df = spark.createDataFrame([("2024-01-01 00:00:00",)], ["created_at"])
    result = add_partition_columns(df)
    assert "created_at" in result.columns  # not dropped
    assert "_ts"        not in result.columns  # temp col cleaned up


# ── enforce_schema ────────────────────────────────────────────────────────────

def test_enforce_schema_casts_types(spark):
    schema = StructType([
        StructField("amount", DoubleType(), True),
        StructField("qty",    IntegerType(), True),
    ])
    df = spark.createDataFrame([("10.5", "3")], ["amount", "qty"])
    result = enforce_schema(df, schema).first()
    assert isinstance(result["amount"], float)
    assert isinstance(result["qty"],    int)

def test_enforce_schema_adds_missing_nullable_col(spark):
    schema = StructType([
        StructField("existing", StringType(), True),
        StructField("missing",  StringType(), True),
    ])
    df = spark.createDataFrame([("val",)], ["existing"])
    result = enforce_schema(df, schema)
    assert "missing" in result.columns
    assert result.first()["missing"] is None


# ── split_quarantine ──────────────────────────────────────────────────────────

def test_quarantine_null_key(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount",   DoubleType(), True),
    ])
    df = spark.createDataFrame([("ORD-1", 10.0), (None, 5.0)], schema)
    clean, quar = split_quarantine(df, not_null_cols=["order_id"])
    assert clean.count() == 1
    assert quar.count()  == 1

def test_quarantine_non_positive(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount",   DoubleType(), True),
    ])
    df = spark.createDataFrame([("ORD-1", 10.0), ("ORD-2", 0.0), ("ORD-3", -1.0)], schema)
    clean, quar = split_quarantine(df, not_null_cols=["order_id"], positive_cols=["amount"])
    assert clean.count() == 1
    assert quar.count()  == 2

def test_quarantine_all_clean(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount",   DoubleType(), True),
    ])
    df = spark.createDataFrame([("ORD-1", 10.0), ("ORD-2", 5.0)], schema)
    clean, quar = split_quarantine(df, not_null_cols=["order_id"], positive_cols=["amount"])
    assert clean.count() == 2
    assert quar.count()  == 0

def test_quarantine_has_reason_column(spark):
    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame([("A",), (None,)], schema)
    _, quar = split_quarantine(df, not_null_cols=["id"])
    assert "_quarantine_reason" in quar.columns
    assert quar.first()["_quarantine_reason"] == "null:id"

def test_quarantine_clean_has_no_reason_column(spark):
    schema = StructType([StructField("id", StringType(), True)])
    df = spark.createDataFrame([("A",)], schema)
    clean, _ = split_quarantine(df, not_null_cols=["id"])
    assert "_quarantine_reason" not in clean.columns


# ── quality_metrics ───────────────────────────────────────────────────────────

def test_quality_metrics_pass_rate(spark):
    raw   = spark.createDataFrame([("a",), ("b",), ("c",)], ["v"])
    clean = spark.createDataFrame([("a",), ("b",)], ["v"])
    quar  = spark.createDataFrame([("c",)], ["v"])
    m = quality_metrics(raw, clean, quar)
    assert m["raw_count"]        == 3
    assert m["clean_count"]      == 2
    assert m["quarantine_count"] == 1
    assert m["pass_rate_pct"]    == 66.67

def test_quality_metrics_empty_raw(spark):
    empty = spark.createDataFrame([], StructType([StructField("v", StringType(), True)]))
    m = quality_metrics(empty, empty, empty)
    assert m["pass_rate_pct"] == 0.0

def test_quality_metrics_all_clean(spark):
    df = spark.createDataFrame([("a",), ("b",)], ["v"])
    empty = spark.createDataFrame([], StructType([StructField("v", StringType(), True)]))
    m = quality_metrics(df, df, empty)
    assert m["pass_rate_pct"] == 100.0
