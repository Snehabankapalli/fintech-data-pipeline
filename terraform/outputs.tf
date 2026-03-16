output "raw_bucket_name"        { value = aws_s3_bucket.raw.bucket }
output "processed_bucket_name"  { value = aws_s3_bucket.processed.bucket }
output "emr_application_id"     { value = aws_emrserverless_application.spark.id }
output "snowflake_warehouse"    { value = snowflake_warehouse.fintech.name }
output "snowflake_stage_url"    { value = snowflake_stage.raw_transactions.url }
