provider "aws" {
  region = var.aws_region
}

# -------------------------------------------------------------------------
# S3 Buckets for Data Lake Layers
# -------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake_raw" {
  bucket = "${var.project_name}-raw-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "datalake_silver" {
  bucket = "${var.project_name}-silver-delta-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "datalake_gold" {
  bucket = "${var.project_name}-gold-analytics-${var.environment}"
  force_destroy = true
}

# -------------------------------------------------------------------------
# AWS Glue Catalog Database
# -------------------------------------------------------------------------
resource "aws_glue_catalog_database" "glue_db" {
  name = "${var.project_name}_db_${var.environment}"
}

# -------------------------------------------------------------------------
# IAM Roles for Glue
# -------------------------------------------------------------------------
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "GlueS3Access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:Get*",
          "s3:List*",
          "s3:Put*"
        ]
        Resource = [
          aws_s3_bucket.datalake_raw.arn,
          "${aws_s3_bucket.datalake_raw.arn}/*",
          aws_s3_bucket.datalake_silver.arn,
          "${aws_s3_bucket.datalake_silver.arn}/*",
          aws_s3_bucket.datalake_gold.arn,
          "${aws_s3_bucket.datalake_gold.arn}/*"
        ]
      }
    ]
  })
}

# -------------------------------------------------------------------------
# Glue Job (PySpark) - Raw to Silver (Delta)
# -------------------------------------------------------------------------
resource "aws_s3_object" "job_script" {
  bucket = aws_s3_bucket.datalake_raw.bucket
  key    = "scripts/process_delta_lake.py"
  source = "../../src/jobs/process_delta_lake.py"
  etag   = filemd5("../../src/jobs/process_delta_lake.py")
}

resource "aws_glue_job" "delta_etl" {
  name     = "${var.project_name}-etl-job"
  role_arn = aws_iam_role.glue_service_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${aws_s3_bucket.datalake_raw.bucket}/scripts/process_delta_lake.py"
    python_version  = "3"
  }

  default_arguments = {
    "--datalake-formats" = "delta"
    "--conf"             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "--raw_bucket"       = aws_s3_bucket.datalake_raw.bucket
    "--silver_bucket"    = aws_s3_bucket.datalake_silver.bucket
  }
}

# -------------------------------------------------------------------------
# Redshift Spectrum (Simulated/Placeholder)
# -------------------------------------------------------------------------
# For real implementation, we would define aws_redshift_cluster here.
# Keeping it minimal for this demonstration to focus on Glue/Delta.
