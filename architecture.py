#!/usr/bin/env python3
"""
AWS Serverless Data Lake ETL — Animated Architecture Diagram
Run:  python architecture.py
Out:  docs/architecture.svg  (open in browser or VS Code SVG preview)

Deps:
    brew install graphviz
    pip install diagrams
"""
import os
import re
from diagrams import Diagram, Cluster, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import Glue, Athena, LakeFormation
from diagrams.aws.management import Cloudwatch
from diagrams.aws.security import IAM
from diagrams.onprem.client import Client

os.makedirs("docs", exist_ok=True)

graph_attr = {
    "fontsize":  "17",
    "fontname":  "Helvetica Neue",
    "pad":       "1.0",
    "splines":   "ortho",
    "nodesep":   "0.9",
    "ranksep":   "1.5",
    "bgcolor":   "#0d1117",
    "fontcolor": "#e6edf3",
}

node_attr = {
    "fontsize":  "12",
    "fontname":  "Helvetica Neue",
    "fontcolor": "#e6edf3",
}

edge_attr = {
    "fontsize":  "10",
    "fontname":  "Helvetica Neue",
    "color":     "#ff9900",
    "fontcolor": "#aaaaaa",
}

# ── Step 1: Generate base SVG ──────────────────────────────────────────────────
with Diagram(
    name="AWS Serverless Data Lake ETL Pipeline",
    filename="docs/architecture",
    outformat="svg",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    sources = Client("Data Sources\nCSV · JSON · API")
    cw      = Cloudwatch("CloudWatch\nLogs & Metrics")

    with Cluster("Phase 1 — Raw Ingestion"):
        s3_raw  = S3("S3 Raw Bucket\nLanding Zone")
        trigger = Lambda("Lambda\nIngestion Trigger\nPython 3.12")

    with Cluster("Phase 2 — Serverless ETL"):
        glue_job     = Glue("Glue ETL Job\nPySpark · Glue 4.0")
        s3_processed = S3("S3 Processed\nParquet · Partitioned")

    with Cluster("Phase 3 — Catalog & Query"):
        crawler = Glue("Glue Crawler\nDaily 06:00 UTC")
        catalog = Glue("Glue Data Catalog\nSchema · Partitions")
        athena  = Athena("Athena\nServerless SQL")

    with Cluster("Phase 4 — Governance & Security"):
        lf  = LakeFormation("Lake Formation\nFine-grained Access")
        iam = IAM("IAM\nRoles & Policies")

    # Main data flow
    sources      >> Edge(label="upload file")                        >> s3_raw
    s3_raw       >> Edge(label="ObjectCreated event")                >> trigger
    trigger      >> Edge(label="StartJobRun (boto3)")                >> glue_job
    glue_job     >> Edge(label="write Parquet\npartitioned by date") >> s3_processed
    s3_processed >> Edge(label="scan new partitions")                >> crawler
    crawler      >> Edge(label="update schema")                      >> catalog
    catalog      >> Edge(label="table metadata")                     >> athena

    # Monitoring
    trigger  >> Edge(style="dashed", color="#555555", label="logs")           >> cw
    glue_job >> Edge(style="dashed", color="#555555", label="metrics + logs") >> cw

    # Governance
    lf >> Edge(style="dashed", color="#1a9c3e", label="manages access") >> s3_raw
    lf >> Edge(style="dashed", color="#1a9c3e")                         >> s3_processed
    lf >> Edge(style="dashed", color="#1a9c3e")                         >> catalog

    # IAM
    iam >> Edge(style="dashed", color="#666666") >> trigger
    iam >> Edge(style="dashed", color="#666666") >> glue_job


# ── Step 2: Inject CSS animations into the SVG ────────────────────────────────
SVG_FILE = "docs/architecture.svg"

CSS = """<style>
  /* ── Background ── */
  .graph > polygon { fill: #0d1117; }

  /* ── Main edges: orange flowing dashes (data in motion) ── */
  g.edge path {
    stroke-dasharray: 12 6;
    animation: dataFlow 1.8s linear infinite;
  }

  /* ── Arrowheads pulse with the flow ── */
  g.edge polygon {
    animation: arrowPulse 1.8s ease-in-out infinite;
  }

  @keyframes dataFlow {
    0%   { stroke-dashoffset: 36; }
    100% { stroke-dashoffset: 0;  }
  }

  @keyframes arrowPulse {
    0%, 100% { opacity: 0.25; }
    60%      { opacity: 1.0;  }
  }

  /* ── Node icons: soft breathe glow ── */
  g.node image {
    animation: nodePulse 3s ease-in-out infinite alternate;
  }

  @keyframes nodePulse {
    from { opacity: 0.78; filter: drop-shadow(0 0 0px rgba(255,153,0,0));   }
    to   { opacity: 1.0;  filter: drop-shadow(0 0 8px rgba(255,153,0,0.5)); }
  }

  /* ── Node labels ── */
  g.node text { fill: #e6edf3; }

  /* ── Cluster boxes: gentle breathe ── */
  g.cluster polygon {
    fill: rgba(255,255,255,0.03);
    stroke: rgba(255,153,0,0.35);
    animation: clusterBreath 4s ease-in-out infinite alternate;
  }

  @keyframes clusterBreath {
    from { stroke-opacity: 0.25; }
    to   { stroke-opacity: 0.65; }
  }

  /* ── Cluster labels ── */
  g.cluster text { fill: #ff9900; font-weight: bold; }

  /* ── Edge labels ── */
  g.edge text { fill: #8b949e; }

  /* ── Graph title ── */
  text.label { fill: #e6edf3; }
</style>
"""

with open(SVG_FILE, "r", encoding="utf-8") as f:
    svg = f.read()

# Inject CSS immediately after the opening <svg ...> tag
svg = re.sub(r"(<svg\b[^>]*>)", r"\1\n" + CSS, svg, count=1)

with open(SVG_FILE, "w", encoding="utf-8") as f:
    f.write(svg)

print("Generated: docs/architecture.svg")
print("→ Open in browser (drag & drop) or VS Code with SVG Preview extension")
