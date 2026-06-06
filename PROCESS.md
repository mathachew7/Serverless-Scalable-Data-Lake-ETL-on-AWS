# Build Process Log — Serverless Scalable Data Lake ETL on AWS

> Every step recorded. Token compact checkpoints marked. Architecture decisions explained.

---

## Session 1 — 2026-06-06

### Context
- Repo initialized with README only
- Building from scratch, vibe coding style
- Goal: production-grade, extensible, cloud-native data lake on AWS

---

## Architecture Decision Record (ADR)

| # | Decision | Reason |
|---|----------|--------|
| 1 | Terraform for IaC | Portable, reusable, version-controlled infra |
| 2 | S3 as data lake storage | Cost-effective, serverless, scales infinitely |
| 3 | Parquet as processed format | Columnar, compressed, Athena-optimized |
| 4 | PySpark on Glue | Managed Spark, no cluster management |
| 5 | Lake Formation for governance | Fine-grained access at table/column/row level |
| 6 | Partition by year/month/day | Query performance + cost optimization in Athena |

---

## Build Phases

### Phase 1: Raw Ingestion Layer — [ ] IN PROGRESS
- [ ] S3 buckets (raw, processed, glue-scripts, athena-results)
- [ ] IAM roles and policies (Lambda, Glue)
- [ ] Lambda ingestion trigger (Python)
- [ ] S3 event notification wiring
- [ ] Sample data (CSV + JSON)

### Phase 2: Serverless ETL — [ ] PENDING
- [ ] Glue ETL script (PySpark)
- [ ] Glue Job definition (Terraform)
- [ ] Data cleansing, type casting, dedup
- [ ] Parquet output, partitioned by date
- [ ] Lambda → Glue trigger wiring

### Phase 3: Catalog & Querying — [ ] PENDING
- [ ] Glue Crawler (Terraform)
- [ ] Glue Database + Table
- [ ] Athena Workgroup config
- [ ] Sample Athena queries

### Phase 4: Governance & Security — [ ] PENDING
- [ ] Lake Formation registration
- [ ] Fine-grained access controls
- [ ] S3 SSE-KMS encryption
- [ ] IAM role → LF permission mapping

---

## Step-by-Step Log

### Step 1 — Project Scaffold [2026-06-06]
**Status:** ✅ Done
**What:** Created folder structure, Terraform skeleton, Lambda scaffold, Glue script scaffold, sample data
**Files created:**
- `terraform/main.tf` — provider, backend stub, locals
- `terraform/variables.tf` — all config knobs (region, env, bucket suffix)
- `terraform/outputs.tf` — all resource outputs
- `terraform/s3.tf` — 4 S3 buckets (raw, processed, glue-scripts, athena-results) + lifecycle + encryption + event notification
- `terraform/iam.tf` — Lambda exec role + Glue service role, least-privilege policies
- `terraform/lambda.tf` — ingestion trigger Lambda wired to S3 events
- `terraform/glue.tf` — Glue ETL job + Crawler + Athena workgroup
- `lambda/ingestion_trigger/handler.py` — validates file, detects format, starts Glue job
- `glue/transform_script.py` — PySpark ETL: clean → deduplicate → partition → Parquet
- `sample_data/orders.csv` — 10 orders rows for testing
- `sample_data/events.json` — 8 clickstream events for testing
- `athena/queries/orders_analysis.sql` — 4 analysis queries
- `athena/queries/events_analysis.sql` — 3 funnel/traffic queries
- `.gitignore`, `terraform.tfvars.example`

### Step 2 — Architecture Diagram (diagrams lib) [2026-06-06]
**Status:** ✅ Done
**What:** `architecture.py` generates `docs/architecture.png` locally via Graphviz — no server, no CDN, fully offline
**Files:** `architecture.py`, `requirements-viz.txt`, `docs/` folder
**Note:** Placeholder — will be replaced with a concrete interactive visualization later
**Run:** `brew install graphviz && pip install diagrams && python architecture.py`

### Step 3 — Terraform Init & Plan [NEXT]
**Status:** ⏳ Pending
**What:** `terraform init` + `terraform plan` to verify infra before deploying

---

> Token compact checkpoint: Compact when PROCESS.md step count > 20 or context feels heavy
