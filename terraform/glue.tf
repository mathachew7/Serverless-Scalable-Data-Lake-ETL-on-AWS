# ─── AWS Glue: ETL Job + Crawler + Data Catalog ───────────────────────────────

# ── Glue Data Catalog Database ────────────────────────────────────────────────
resource "aws_glue_catalog_database" "datalake" {
  name        = replace("${local.name_prefix}_db", "-", "_")
  description = "Data Catalog for the ${var.project_name} data lake"
}

# ── Glue ETL Job ──────────────────────────────────────────────────────────────
resource "aws_glue_job" "etl_transform" {
  name              = "${local.name_prefix}-etl-transform"
  description       = "PySpark ETL: raw CSV/JSON → processed Parquet with partitioning"
  role_arn          = aws_iam_role.glue_service.arn
  glue_version      = var.glue_spark_version
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/transform_script.py"
    python_version  = var.glue_python_version
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
    "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
    "--PROCESSED_BUCKET"                 = aws_s3_bucket.processed.bucket
    "--DATABASE_NAME"                    = aws_glue_catalog_database.datalake.name
    "--ENVIRONMENT"                      = var.environment
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/tmp/"
  }

  execution_property {
    max_concurrent_runs = 3
  }
}

# ── Glue Crawler (processed data) ─────────────────────────────────────────────
resource "aws_glue_crawler" "processed" {
  name          = "${local.name_prefix}-processed-crawler"
  description   = "Crawls processed S3 data to infer schema and update Glue Catalog"
  role          = aws_iam_role.glue_service.arn
  database_name = aws_glue_catalog_database.datalake.name
  schedule      = "cron(0 6 * * ? *)" # daily at 06:00 UTC

  s3_target {
    path = "s3://${aws_s3_bucket.processed.bucket}/data/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

# ── Athena Workgroup ──────────────────────────────────────────────────────────
resource "aws_athena_workgroup" "main" {
  name        = "${local.name_prefix}-workgroup"
  description = "Athena workgroup for ${var.project_name} queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
