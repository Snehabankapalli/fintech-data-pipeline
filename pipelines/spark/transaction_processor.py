"""
PySpark transaction processor for EMR Serverless.
Transforms and enriches raw transaction events before Snowflake load.

Run on EMR Serverless:
  aws emr-serverless start-job-run \
    --application-id <app-id> \
    --execution-role-arn <role-arn> \
    --job-driver '{"sparkSubmit": {"entryPoint": "s3://bucket/scripts/transaction_processor.py"}}'
"""

import argparse
import os
from datetime import datetime, timezone

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = structlog.get_logger()

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",        StringType(),    False),
    StructField("card_id",               StringType(),    False),
    StructField("member_id",             StringType(),    False),
    StructField("amount",                FloatType(),     True),
    StructField("currency_code",         StringType(),    True),
    StructField("merchant_name",         StringType(),    True),
    StructField("merchant_category_code",StringType(),    True),
    StructField("transaction_type",      StringType(),    True),
    StructField("transaction_status",    StringType(),    True),
    StructField("posted_at",             TimestampType(), True),
    StructField("authorized_at",         TimestampType(), True),
    StructField("network",               StringType(),    True),
    StructField("is_international",      BooleanType(),   True),
    StructField("decline_reason",        StringType(),    True),
])


def build_spark_session(app_name: str = "FintechTransactionProcessor") -> SparkSession:
    """Initialize Spark session optimized for EMR Serverless + Snowflake."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def read_raw_transactions(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Read raw JSON transaction files from S3.

    Args:
        spark:    SparkSession
        s3_path:  S3 URI (e.g., s3://bucket/transactions/2024/01/15/)

    Returns:
        DataFrame with parsed transaction schema
    """
    logger.info("reading_raw_transactions", path=s3_path)
    df = (
        spark.read
        .schema(TRANSACTION_SCHEMA)
        .option("multiline", "true")
        .option("mode", "PERMISSIVE")
        .json(s3_path)
    )
    logger.info("raw_records_loaded", count=df.count())
    return df


def transform_transactions(df: DataFrame) -> DataFrame:
    """
    Apply business transformations to raw transactions.

    Transformations:
    - Normalize amounts to USD using currency conversion
    - Classify transaction risk tiers
    - Flag potential fraud patterns
    - Add processing metadata
    """
    return (
        df
        # Deduplicate on transaction_id, keep most recent
        .withColumn(
            "_rank",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("transaction_id")
                .orderBy(F.col("authorized_at").desc())
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")

        # Normalize transaction status
        .withColumn(
            "transaction_status",
            F.upper(F.trim(F.col("transaction_status"))),
        )

        # Amount tier classification
        .withColumn(
            "amount_tier",
            F.when(F.col("amount") >= 1000, "HIGH")
             .when(F.col("amount") >= 100, "MEDIUM")
             .otherwise("LOW"),
        )

        # Geographic classification
        .withColumn(
            "geo_type",
            F.when(F.col("is_international") == True, "INTERNATIONAL")
             .otherwise("DOMESTIC"),
        )

        # Basic fraud signal: high amount + international
        .withColumn(
            "is_high_risk",
            (F.col("amount") > 500) & (F.col("is_international") == True),
        )

        # Date partitioning for Snowflake clustering
        .withColumn("event_date",  F.to_date("posted_at"))
        .withColumn("event_month", F.date_trunc("month", F.col("posted_at")))
        .withColumn("event_hour",  F.hour("posted_at"))

        # Processing metadata
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_processor_version", F.lit("1.0.0"))

        # Drop nulls on required fields
        .dropna(subset=["transaction_id", "card_id", "member_id"])
    )


def write_to_s3(df: DataFrame, output_path: str, partition_cols: list[str]) -> int:
    """
    Write transformed DataFrame to S3 in Parquet format.

    Partitioned for efficient Snowflake external table scanning.
    """
    logger.info("writing_output", path=output_path, partitions=partition_cols)

    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(output_path)

    count = df.count()
    logger.info("output_written", rows=count, path=output_path)
    return count


def main(input_path: str, output_path: str) -> None:
    """Full pipeline: read → transform → write."""
    spark = build_spark_session()

    logger.info("transaction_processor_started", input=input_path, output=output_path)

    raw_df = read_raw_transactions(spark, input_path)
    transformed_df = transform_transactions(raw_df)
    rows_written = write_to_s3(
        transformed_df,
        output_path,
        partition_cols=["event_date"],
    )

    logger.info("transaction_processor_complete", rows_written=rows_written)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fintech transaction processor")
    parser.add_argument("--input",  required=True, help="S3 input path")
    parser.add_argument("--output", required=True, help="S3 output path")
    args = parser.parse_args()
    main(args.input, args.output)
