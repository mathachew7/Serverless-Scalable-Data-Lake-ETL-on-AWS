# ─── S3 Buckets ───────────────────────────────────────────────────────────────
# 4 buckets: raw, processed, glue-scripts, athena-results
# All private, versioned, server-side encrypted

locals {
  buckets = {
    raw            = "${local.name_prefix}-raw-${var.bucket_suffix}"
    processed      = "${local.name_prefix}-processed-${var.bucket_suffix}"
    glue_scripts   = "${local.name_prefix}-glue-scripts-${var.bucket_suffix}"
    athena_results = "${local.name_prefix}-athena-results-${var.bucket_suffix}"
  }
}

# ── Raw Data Lake (landing zone) ──────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = local.buckets.raw
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "archive-old-raw-data"
    status = "Enabled"
    filter { prefix = "" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ── Processed Data Lake ───────────────────────────────────────────────────────
resource "aws_s3_bucket" "processed" {
  bucket = local.buckets.processed
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Glue Scripts ──────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "glue_scripts" {
  bucket = local.buckets.glue_scripts
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload the Glue ETL script
resource "aws_s3_object" "glue_transform_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/transform_script.py"
  source = "${path.module}/../glue/transform_script.py"
  etag   = filemd5("${path.module}/../glue/transform_script.py")
}

# ── Athena Query Results ───────────────────────────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket = local.buckets.athena_results
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = 30 }
  }
}

# ── S3 Event Notification → Lambda ────────────────────────────────────────────
resource "aws_s3_bucket_notification" "raw_upload_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "uploads/"
    filter_suffix       = ""
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}
