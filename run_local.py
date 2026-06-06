#!/usr/bin/env python3
"""
Local pipeline simulation — full ETL run on sample data, no AWS needed.

Run:  python run_local.py
Out:  output/processed/   (Parquet — clean data)
      output/quarantine/  (Parquet — bad rows with reason column)
"""
import os
import sys
import shutil

sys.path.insert(0, "glue")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from transforms import (
    add_partition_columns,
    clean_column_names,
    deduplicate,
    drop_all_null_rows,
    enforce_schema,
    quality_metrics,
    split_quarantine,
)
from schemas import SCHEMA_MAP

# ── Helpers ───────────────────────────────────────────────────────────────────

W  = 62

def banner(title):
    print(f"\n{'━' * W}")
    print(f"  {title}")
    print(f"{'━' * W}")

def step(n, msg):   print(f"\n  [{n}] {msg}")
def ok(msg):        print(f"      ✓  {msg}")
def warn(msg):      print(f"      ⚠  {msg}")
def info(msg):      print(f"      →  {msg}")

# ── Bootstrap ─────────────────────────────────────────────────────────────────

banner("AWS Serverless Data Lake ETL  —  Local Run")
print(f"  Simulates the full pipeline on your machine using PySpark.")
print(f"  No AWS credentials needed.")

if os.path.exists("output"):
    shutil.rmtree("output")
os.makedirs("output/processed",  exist_ok=True)
os.makedirs("output/quarantine", exist_ok=True)

step("0", "Starting local SparkSession …")
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("DataLakeETL-LocalRun")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
ok("SparkSession ready  (local[*]  — using all CPU cores)")

# ── Inject some intentionally bad rows to demo quarantine ────────────────────
# In real life these arrive naturally — null IDs, zero amounts, corrupt rows.

BAD_ORDERS_SCHEMA = StructType([
    StructField("order_id",     StringType(),  True),
    StructField("customer_id",  StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("total_amount", DoubleType(),  True),
    StructField("status",       StringType(),  True),
    StructField("created_at",   StringType(),  True),
])

bad_orders = spark.createDataFrame([
    (None,     "CUST-999", "Broken Widget", 1,    9.99,  9.99,  "pending",   "2024-01-20 08:00:00"),
    ("ORD-DUP","CUST-101", "Wireless Headphones", 2, 79.99, 159.98, "completed", "2024-01-15 10:30:00"),
    ("ORD-DUP","CUST-101", "Wireless Headphones", 2, 79.99, 159.98, "completed", "2024-01-15 10:30:00"),
    ("ORD-BAD","CUST-888", "Free Stuff",    0,    0.0,  -5.00, "fraud",     "2024-01-21 00:00:00"),
], BAD_ORDERS_SCHEMA)

info("Injected 4 intentionally bad/duplicate rows into orders to demo quarantine")

# ── Process each source ───────────────────────────────────────────────────────

SOURCES = [
    ("sample_data/orders.csv",  "csv",  "orders",  bad_orders),
    ("sample_data/events.json", "json", "events",  None),
]

summary = {}

for path, fmt, name, extra_bad in SOURCES:

    banner(f"Source: {name.upper()}  ({fmt.upper()} → Parquet)")
    schema, not_null_cols, positive_cols = SCHEMA_MAP[fmt]

    # ── EXTRACT ───────────────────────────────────────────────────────────
    step("1", f"Reading raw {fmt.upper()} from {path}")

    if fmt == "csv":
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        if extra_bad:
            df_raw = df_raw.unionByName(extra_bad, allowMissingColumns=True)
    else:
        df_raw = spark.read.option("multiline", "true").json(path)

    raw_count = df_raw.count()
    ok(f"{raw_count} rows loaded")
    info("Schema detected:")
    for f in df_raw.schema.fields:
        print(f"         {f.name:<22} {str(f.dataType):<18} nullable={f.nullable}")

    # ── TRANSFORM ─────────────────────────────────────────────────────────
    step("2", "Normalising column names (lower + snake_case)")
    df = clean_column_names(df_raw)
    ok(f"Columns: {df.columns}")

    step("3", "Deduplication")
    before = df.count()
    df = deduplicate(df)
    after  = df.count()
    removed = before - after
    ok(f"{after} rows kept  ({removed} duplicate(s) removed)")

    step("4", "Dropping all-null rows")
    df = drop_all_null_rows(df)
    ok(f"{df.count()} rows remaining")

    if schema:
        step("5", "Enforcing schema (cast types, add missing columns)")
        df = enforce_schema(df, schema)
        ok("Schema enforced")

    step("6", "Adding partition columns  →  year / month / day")
    df = add_partition_columns(df)
    sample = df.first()
    ok(f"Partition from first row:  year={sample['year']}  month={sample['month']}  day={sample['day']}")

    # ── DATA QUALITY ──────────────────────────────────────────────────────
    step("7", f"Data quality checks   not-null: {not_null_cols}   positive: {positive_cols}")
    clean_df, quarantine_df = split_quarantine(df, not_null_cols, positive_cols)
    metrics = quality_metrics(df_raw, clean_df, quarantine_df)

    ok(f"Raw: {metrics['raw_count']}  →  Clean: {metrics['clean_count']}  →  Quarantine: {metrics['quarantine_count']}")
    ok(f"Pass rate: {metrics['pass_rate_pct']}%")
    summary[name] = metrics

    if quarantine_df.count() > 0:
        warn(f"{quarantine_df.count()} rows quarantined:")
        quarantine_df.select("_quarantine_reason").groupBy("_quarantine_reason").count().show(truncate=False)

    # ── LOAD ──────────────────────────────────────────────────────────────
    step("8", f"Writing clean Parquet  →  output/processed/{name}/")
    (
        clean_df
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(f"output/processed/{name}")
    )
    ok("Written — partitioned by year / month / day")

    if quarantine_df.count() > 0:
        step("9", f"Writing quarantine Parquet  →  output/quarantine/{name}/")
        quarantine_df.write.mode("overwrite").parquet(f"output/quarantine/{name}")
        ok(f"Written {quarantine_df.count()} quarantine row(s)")

# ── Read back & preview ───────────────────────────────────────────────────────

banner("Reading back processed output  (what Athena would query)")

for name in ["orders", "events"]:
    step("→", f"{name} — clean Parquet")
    df_out = spark.read.parquet(f"output/processed/{name}")
    ok(f"{df_out.count()} rows, {len(df_out.columns)} columns")
    df_out.show(5, truncate=True)

    partitions = [r.asDict() for r in df_out.select("year","month","day").distinct().collect()]
    info(f"Partitions: {partitions}")

# ── File tree ─────────────────────────────────────────────────────────────────

banner("Output file tree")
for root, dirs, files in os.walk("output"):
    dirs.sort()
    level   = root.replace("output", "").count(os.sep)
    indent  = "  " * level
    print(f"{indent}{os.path.basename(root)}/")
    for file in sorted(files):
        fpath = os.path.join(root, file)
        size  = os.path.getsize(fpath)
        print(f"{'  ' * (level+1)}{file}   ({size/1024:.1f} KB)")

# ── Summary ───────────────────────────────────────────────────────────────────

banner("Run Summary")
for name, m in summary.items():
    status = "✓" if m["pass_rate_pct"] >= 80 else "⚠"
    print(f"  {status}  {name:<10}  raw={m['raw_count']}  clean={m['clean_count']}  "
          f"quarantine={m['quarantine_count']}  pass={m['pass_rate_pct']}%")

print(f"\n  Local run complete.")
print(f"  Next step → deploy to AWS:  make init && make plan && make apply")
print(f"{'━' * W}\n")

spark.stop()
