variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for all resource names"
  type        = string
  default     = "datalake-etl"
}

variable "environment" {
  description = "Deployment environment (dev/staging/prod)"
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner tag for all resources"
  type        = string
  default     = "mathachew7"
}

# S3 bucket suffix — must be globally unique
variable "bucket_suffix" {
  description = "Unique suffix appended to all S3 bucket names (e.g. your AWS account alias)"
  type        = string
  default     = "subash"
}

variable "glue_python_version" {
  description = "Python version for Glue ETL jobs"
  type        = string
  default     = "3"
}

variable "glue_spark_version" {
  description = "Glue version (maps to Spark version)"
  type        = string
  default     = "4.0"
}

variable "lambda_runtime" {
  description = "Lambda Python runtime"
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 60
}

variable "lambda_memory" {
  description = "Lambda memory in MB"
  type        = number
  default     = 256
}
