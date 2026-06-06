#!/usr/bin/env python3
"""
AWS Serverless Data Lake ETL — Architecture Diagram
Run:  python architecture.py
Out:  docs/architecture.png

Deps:
    brew install graphviz
    pip install diagrams
"""
import os
from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import Glue, Athena, LakeFormation
from diagrams.aws.management import Cloudwatch
from diagrams.aws.security import IAM
from diagrams.onprem.client import Client

os.makedirs("docs", exist_ok=True)

# ── Graph-level visual config ──────────────────────────────────────────────────
graph_attr = {
    "fontsize":  "17",
    "fontname":  "Helvetica Neue",
    "pad":       "1.0",
    "splines":   "ortho",
    "nodesep":   "0.9",
    "ranksep":   "1.5",
    "bgcolor":   "#ffffff",
}

node_attr = {
    "fontsize": "12",
    "fontname": "Helvetica Neue",
}

edge_attr = {
    "fontsize": "10",
    "fontname": "Helvetica Neue",
    "color":    "#555555",
    "fontcolor":"#444444",
}

# ── Diagram ────────────────────────────────────────────────────────────────────
with Diagram(
    name="AWS Serverless Data Lake ETL Pipeline",
    filename="docs/architecture",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):

    # ── External: Data Sources ──────────────────────────────────────────────
    sources = Client("Data Sources\nCSV · JSON · API")

    # ── Observability (shared) ──────────────────────────────────────────────
    cw = Cloudwatch("CloudWatch\nLogs & Metrics")

    # ── Phase 1: Raw Ingestion ──────────────────────────────────────────────
    with Cluster("Phase 1 — Raw Ingestion"):
        s3_raw   = S3("S3 Raw Bucket\nLanding Zone")
        trigger  = Lambda("Lambda\nIngestion Trigger\nPython 3.12")

    # ── Phase 2: Serverless ETL ─────────────────────────────────────────────
    with Cluster("Phase 2 — Serverless ETL"):
        glue_job     = Glue("Glue ETL Job\nPySpark · Glue 4.0")
        s3_processed = S3("S3 Processed\nParquet · Partitioned")

    # ── Phase 3: Catalog & Query ────────────────────────────────────────────
    with Cluster("Phase 3 — Catalog & Query"):
        crawler  = Glue("Glue Crawler\nDaily 06:00 UTC")
        catalog  = Glue("Glue Data Catalog\nSchema · Partitions")
        athena   = Athena("Athena\nServerless SQL")

    # ── Phase 4: Governance & Security ─────────────────────────────────────
    with Cluster("Phase 4 — Governance & Security"):
        lf  = LakeFormation("Lake Formation\nFine-grained Access")
        iam = IAM("IAM\nRoles & Policies")

    # ── Main data flow ──────────────────────────────────────────────────────
    sources      >> Edge(label="upload file")              >> s3_raw
    s3_raw       >> Edge(label="ObjectCreated event")      >> trigger
    trigger      >> Edge(label="StartJobRun (boto3)")      >> glue_job
    glue_job     >> Edge(label="write Parquet\npartitioned by date") >> s3_processed
    s3_processed >> Edge(label="scan new partitions")      >> crawler
    crawler      >> Edge(label="update schema")            >> catalog
    catalog      >> Edge(label="table metadata")           >> athena

    # ── Monitoring (dashed) ─────────────────────────────────────────────────
    trigger  >> Edge(style="dashed", color="#aaaaaa", label="logs") >> cw
    glue_job >> Edge(style="dashed", color="#aaaaaa", label="metrics + logs") >> cw

    # ── Governance (dashed green) ───────────────────────────────────────────
    lf >> Edge(style="dashed", color="#1a9c3e", label="manages access") >> s3_raw
    lf >> Edge(style="dashed", color="#1a9c3e")                         >> s3_processed
    lf >> Edge(style="dashed", color="#1a9c3e")                         >> catalog

    # ── IAM wires (dashed gray) ─────────────────────────────────────────────
    iam >> Edge(style="dashed", color="#999999") >> trigger
    iam >> Edge(style="dashed", color="#999999") >> glue_job
