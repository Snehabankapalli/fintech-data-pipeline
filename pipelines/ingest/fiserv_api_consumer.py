"""
Fiserv Payment Processor API consumer.
Pulls transaction and payment events and lands them in S3 for downstream processing.

Supports paginated batch pulls and incremental extraction.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
import requests
import structlog
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

load_dotenv()
logger = structlog.get_logger()


@dataclass
class ExtractionResult:
    """Result of a single API extraction run."""

    endpoint: str
    records_extracted: int
    files_written: int
    s3_prefix: str
    status: str


class FiservAPIConsumer:
    """
    Extracts transaction and payment events from Fiserv APIs.

    Handles:
    - OAuth2 token refresh
    - Paginated batch extraction
    - Partitioned S3 landing (year/month/day/hour)
    - Retry logic for transient API failures
    """

    BASE_URL = os.environ.get("FISERV_API_BASE_URL", "https://api.fiserv.com/v1")
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 10000))

    def __init__(self):
        self._s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
        self._bucket = os.environ["S3_RAW_BUCKET"]
        self._token: str | None = None
        self._token_expiry: datetime | None = None

    def extract_transactions(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> ExtractionResult:
        """
        Extract credit card transactions for a time window.

        Args:
            start_time: Inclusive start (UTC)
            end_time:   Exclusive end (UTC)

        Returns:
            ExtractionResult with record counts and S3 location
        """
        return self._extract(
            endpoint="/creditcard/transactions",
            params={
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "pageSize": self.BATCH_SIZE,
            },
            s3_prefix="transactions",
        )

    def extract_payments(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> ExtractionResult:
        """Extract ACH/wire/bill-pay events for a time window."""
        return self._extract(
            endpoint="/payments/events",
            params={
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "eventTypes": "ACH,WIRE,BILLPAY",
                "pageSize": self.BATCH_SIZE,
            },
            s3_prefix="payments",
        )

    def _extract(
        self, endpoint: str, params: dict, s3_prefix: str
    ) -> ExtractionResult:
        """Generic paginated extraction with S3 landing."""
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        records = []
        page_token = None
        page = 0

        logger.info("api_extraction_started", endpoint=endpoint, params=params)

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self._get_page(endpoint, params, headers)
            batch = response.get("data", [])
            records.extend(batch)
            page += 1

            logger.debug("page_fetched", endpoint=endpoint, page=page, count=len(batch))

            page_token = response.get("nextPageToken")
            if not page_token or not batch:
                break

        if not records:
            logger.warning("no_records_extracted", endpoint=endpoint)
            return ExtractionResult(endpoint, 0, 0, s3_prefix, "empty")

        files_written = self._write_to_s3(records, s3_prefix)

        logger.info(
            "api_extraction_complete",
            endpoint=endpoint,
            records=len(records),
            files=files_written,
        )

        return ExtractionResult(
            endpoint=endpoint,
            records_extracted=len(records),
            files_written=files_written,
            s3_prefix=s3_prefix,
            status="success",
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.HTTPError),
    )
    def _get_page(self, endpoint: str, params: dict, headers: dict) -> dict:
        """Fetch a single page from the Fiserv API with retry."""
        response = requests.get(
            f"{self.BASE_URL}{endpoint}",
            params=params,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _get_token(self) -> str:
        """Fetch or return cached OAuth2 token."""
        now = datetime.now(timezone.utc)
        if self._token and self._token_expiry and self._token_expiry > now:
            return self._token

        response = requests.post(
            f"{self.BASE_URL}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": os.environ["FISERV_CLIENT_ID"],
                "client_secret": os.environ["FISERV_CLIENT_SECRET"],
            },
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()

        self._token = data["access_token"]
        self._token_expiry = now + timedelta(seconds=data.get("expires_in", 3600) - 60)
        return self._token

    def _write_to_s3(self, records: list[dict], prefix: str) -> int:
        """
        Write records to S3 in date-partitioned JSON files.
        Splits into chunks of BATCH_SIZE to keep file sizes manageable.
        """
        now = datetime.now(timezone.utc)
        partition = now.strftime("%Y/%m/%d/%H")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        chunks = [records[i:i + self.BATCH_SIZE] for i in range(0, len(records), self.BATCH_SIZE)]
        files_written = 0

        for idx, chunk in enumerate(chunks):
            key = f"{prefix}/{partition}/{timestamp}_{idx:04d}.json"
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=json.dumps(chunk).encode("utf-8"),
                ContentType="application/json",
            )
            files_written += 1
            logger.debug("s3_file_written", key=key, records=len(chunk))

        return files_written
