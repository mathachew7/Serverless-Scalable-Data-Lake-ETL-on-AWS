from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

# ── Orders (CSV) ──────────────────────────────────────────────────────────────
ORDERS_SCHEMA = StructType([
    StructField("order_id",     StringType(),    False),
    StructField("customer_id",  StringType(),    False),
    StructField("product_name", StringType(),    True),
    StructField("quantity",     IntegerType(),   True),
    StructField("unit_price",   DoubleType(),    True),
    StructField("total_amount", DoubleType(),    True),
    StructField("status",       StringType(),    True),
    StructField("created_at",   TimestampType(), True),
])
ORDERS_NOT_NULL = ["order_id", "customer_id"]
ORDERS_POSITIVE = ["quantity", "total_amount"]

# ── Events (JSON) ─────────────────────────────────────────────────────────────
EVENTS_SCHEMA = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("user_id",    StringType(),    False),
    StructField("event_type", StringType(),    True),
    StructField("page",       StringType(),    True),
    StructField("session_id", StringType(),    True),
    StructField("device",     StringType(),    True),
    StructField("country",    StringType(),    True),
    StructField("timestamp",  TimestampType(), True),
    StructField("order_id",   StringType(),    True),  # present only on purchase events
])
EVENTS_NOT_NULL = ["event_id", "user_id"]
EVENTS_POSITIVE = []

# ── Dispatch by file format ───────────────────────────────────────────────────
SCHEMA_MAP = {
    "csv":     (ORDERS_SCHEMA, ORDERS_NOT_NULL, ORDERS_POSITIVE),
    "json":    (EVENTS_SCHEMA, EVENTS_NOT_NULL, EVENTS_POSITIVE),
    "parquet": (None, [], []),
}
