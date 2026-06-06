output "raw_bucket_name" {
  description = "S3 raw data lake bucket name"
  value       = aws_s3_bucket.raw.bucket
}

output "processed_bucket_name" {
  description = "S3 processed data lake bucket name"
  value       = aws_s3_bucket.processed.bucket
}

output "glue_scripts_bucket_name" {
  description = "S3 bucket where Glue ETL scripts are stored"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "athena_results_bucket_name" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.bucket
}

output "lambda_function_name" {
  description = "Ingestion trigger Lambda function name"
  value       = aws_lambda_function.ingestion_trigger.function_name
}

output "lambda_function_arn" {
  description = "Ingestion trigger Lambda function ARN"
  value       = aws_lambda_function.ingestion_trigger.arn
}

output "glue_job_name" {
  description = "Glue ETL job name"
  value       = aws_glue_job.etl_transform.name
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.datalake.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}
