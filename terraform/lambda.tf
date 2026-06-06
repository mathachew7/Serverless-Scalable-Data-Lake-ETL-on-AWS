# ─── Lambda: Ingestion Trigger ────────────────────────────────────────────────

data "archive_file" "ingestion_trigger" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/ingestion_trigger"
  output_path = "${path.module}/../lambda/ingestion_trigger.zip"
}

resource "aws_lambda_function" "ingestion_trigger" {
  function_name    = "${local.name_prefix}-ingestion-trigger"
  description      = "Triggered on S3 upload; validates and starts Glue ETL job"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "handler.lambda_handler"
  runtime          = var.lambda_runtime
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory

  filename         = data.archive_file.ingestion_trigger.output_path
  source_code_hash = data.archive_file.ingestion_trigger.output_base64sha256

  environment {
    variables = {
      GLUE_JOB_NAME       = "${local.name_prefix}-etl-transform"
      RAW_BUCKET          = aws_s3_bucket.raw.bucket
      PROCESSED_BUCKET    = aws_s3_bucket.processed.bucket
      ENVIRONMENT         = var.environment
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_exec,
    aws_iam_role_policy.lambda_s3_glue
  ]
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_cloudwatch_log_group" "lambda_ingestion" {
  name              = "/aws/lambda/${aws_lambda_function.ingestion_trigger.function_name}"
  retention_in_days = 14
}
