"""
S3 → Snowflake ingestion pipeline.
Loads raw JSON files from S3 into Snowflake external stage via COPY INTO.

Supports batch and continuous loading modes.
Mirrors production pattern used at SoFi for credit card event ingestion.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
import structlog
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from pipelines.snowflake_loader import SnowflakeLoader

load_dotenv()
logger = structlog.get_logger()


@dataclass
class LoadResult:
    """Result of a single S3 → Snowflake load operation."""

    files_processed: int
    rows_loaded: int
    rows_errored: int
    duration_seconds: float
    status: str


class S3ToSnowflakePipeline:
    """
    Loads JSON files from S3 into a Snowflake raw table via COPY INTO.

    Features:
    - Automatic file discovery by S3 prefix and date partition
    - Zero-copy staging via Snowflake external stages
    - Idempotent loads (COPY skips already-loaded files)
    - Structured logging for observability
    """

    def __init__(self, stage_name: str, target_table: str):
        self._stage = stage_name
        self._table = target_table
        self._s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
        self._loader = SnowflakeLoader()

    def run(
        self,
        s3_prefix: str,
        date: datetime | None = None,
        file_format: str = "JSON",
    ) -> LoadResult:
        """
        Execute S3 → Snowflake load for a given prefix and date partition.

        Args:
            s3_prefix:   S3 key prefix (e.g., 'transactions/')
            date:        Date partition to load (defaults to today UTC)
            file_format: Source format ('JSON', 'PARQUET', 'CSV')

        Returns:
            LoadResult with row counts and status
        """
        target_date = date or datetime.now(timezone.utc)
        partition = target_date.strftime("%Y/%m/%d")
        full_prefix = f"{s3_prefix}{partition}/"

        logger.info(
            "s3_load_started",
            stage=self._stage,
            table=self._table,
            prefix=full_prefix,
        )

        start = datetime.now(timezone.utc)

        file_count = self._count_s3_files(full_prefix)
        if file_count == 0:
            logger.warning("no_files_found", prefix=full_prefix)
            return LoadResult(0, 0, 0, 0.0, "no_files")

        result = self._copy_into_snowflake(full_prefix, file_format)

        duration = (datetime.now(timezone.utc) - start).total_seconds()

        logger.info(
            "s3_load_complete",
            table=self._table,
            files=result["files"],
            rows_loaded=result["rows_loaded"],
            rows_errored=result["rows_errored"],
            duration_seconds=round(duration, 2),
        )

        return LoadResult(
            files_processed=result["files"],
            rows_loaded=result["rows_loaded"],
            rows_errored=result["rows_errored"],
            duration_seconds=duration,
            status="success" if result["rows_errored"] == 0 else "partial",
        )

    def _count_s3_files(self, prefix: str) -> int:
        """Count files available in S3 prefix."""
        bucket = os.environ["S3_RAW_BUCKET"]
        paginator = self._s3.get_paginator("list_objects_v2")
        count = sum(
            len(page.get("Contents", []))
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        )
        return count

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=30),
    )
    def _copy_into_snowflake(self, prefix: str, file_format: str) -> dict:
        """
        Execute COPY INTO command for the given prefix.

        Snowflake COPY INTO is idempotent — files already loaded are skipped.
        Uses STRIP_OUTER_ARRAY for JSON arrays from Fiserv API responses.
        """
        copy_sql = f"""
            COPY INTO {self._table} (RAW_PAYLOAD, LOADED_AT)
            FROM (
                SELECT
                    $1::VARIANT,
                    CURRENT_TIMESTAMP()
                FROM @{self._stage}/{prefix}
            )
            FILE_FORMAT = (
                TYPE = '{file_format}'
                STRIP_OUTER_ARRAY = TRUE
                IGNORE_UTF8_ERRORS = TRUE
            )
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE
        """

        results = self._loader.query(copy_sql)

        rows_loaded = int(results["rows_loaded"].sum()) if not results.empty else 0
        rows_errored = int(results["rows_parse_error"].sum()) if not results.empty else 0
        files = len(results) if not results.empty else 0

        return {"files": files, "rows_loaded": rows_loaded, "rows_errored": rows_errored}
