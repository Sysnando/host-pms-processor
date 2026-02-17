"""AWS S3 Manager for uploading raw and processed data."""

import json
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel
from structlog import get_logger

from src.aws.client_factory import get_boto3_client_kwargs
from src.config import settings

logger = get_logger(__name__)


class S3UploadError(Exception):
    """Raised when S3 upload fails."""

    pass


class S3Manager:
    """Manages uploads to raw and processed S3 buckets."""

    def __init__(self):
        """Initialize S3 Manager with AWS settings.

        Uses AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY if set; otherwise
        boto3 default credential provider (SSO, role, etc.).
        """
        self.region = settings.aws.region
        self.s3_client = boto3.client("s3", **get_boto3_client_kwargs("s3"))
        self.raw_prefix = settings.aws_s3_raw_prefix
        self.processed_prefix = settings.aws_s3_processed_prefix
        self.timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    def _get_bucket_name(self, prefix: str, data_type: str) -> str:
        """Construct S3 bucket name from prefix and data type.

        Args:
            prefix: Base prefix (raw or processed)
            data_type: Type of data (hotel-configs, reservations, etc.)

        Returns:
            S3 bucket name
        """
        return f"{prefix}{data_type}"

    def _serialize_data(self, data: Any) -> str:
        """Serialize data to JSON string.

        Handles both Pydantic models and dicts.

        Args:
            data: Data to serialize (dict or Pydantic model)

        Returns:
            JSON string
        """
        if isinstance(data, BaseModel):
            return data.model_dump_json(by_alias=True, indent=2)
        elif isinstance(data, dict):
            return json.dumps(data, indent=2, default=str)
        elif isinstance(data, list):
            return json.dumps(data, indent=2, default=str)
        else:
            return json.dumps({"data": str(data)}, indent=2)

    def upload_raw(
        self,
        hotel_code: str,
        data_type: str,
        data: Any,
        custom_suffix: str = "",
    ) -> dict[str, str]:
        """Upload raw data to raw S3 bucket.

        Raw buckets store original API responses for audit trail.

        Args:
            hotel_code: Hotel code identifier
            data_type: Type of data (hotel-configs, reservations, inventory, revenue)
            data: Data to upload (dict or Pydantic model)
            custom_suffix: Optional custom suffix for filename

        Returns:
            Dictionary with 'key' and 'url' of uploaded file

        Raises:
            S3UploadError: If upload fails
        """
        logger.info(
            "Uploading raw data to S3",
            hotel_code=hotel_code,
            data_type=data_type,
        )

        try:
            bucket_name = self._get_bucket_name(self.raw_prefix, data_type)
            suffix = custom_suffix or self.timestamp
            key = f"{hotel_code}/{data_type}-{suffix}.json"

            # Serialize data
            body = self._serialize_data(data)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "hotel-code": hotel_code,
                    "data-type": data_type,
                    "upload-timestamp": datetime.utcnow().isoformat(),
                },
            )

            url = f"s3://{bucket_name}/{key}"
            logger.info(
                "Successfully uploaded raw data",
                hotel_code=hotel_code,
                data_type=data_type,
                key=key,
                url=url,
            )

            return {"key": key, "url": url}

        except ClientError as e:
            logger.error(
                "Failed to upload raw data to S3",
                hotel_code=hotel_code,
                data_type=data_type,
                error=str(e),
            )
            raise S3UploadError(
                f"Failed to upload raw data for {hotel_code}/{data_type}: {str(e)}"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error uploading raw data",
                hotel_code=hotel_code,
                data_type=data_type,
                error=str(e),
            )
            raise S3UploadError(
                f"Unexpected error uploading raw data: {str(e)}"
            ) from e

    def upload_processed(
        self,
        hotel_code: str,
        data_type: str,
        data: Any,
        custom_suffix: str = "",
    ) -> dict[str, str]:
        """Upload processed data to processed S3 bucket.

        Processed buckets store Climber standardized format data.

        Args:
            hotel_code: Hotel code identifier
            data_type: Type of data (hotel-configs, reservations, inventory, revenue)
            data: Data to upload (dict or Pydantic model)
            custom_suffix: Optional custom suffix for filename

        Returns:
            Dictionary with 'key' and 'url' of uploaded file

        Raises:
            S3UploadError: If upload fails
        """
        logger.info(
            "Uploading processed data to S3",
            hotel_code=hotel_code,
            data_type=data_type,
        )

        try:
            bucket_name = self._get_bucket_name(self.processed_prefix, data_type)
            suffix = custom_suffix or self.timestamp
            key = f"{hotel_code}/{data_type}-{suffix}.json"

            # Serialize data
            body = self._serialize_data(data)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "hotel-code": hotel_code,
                    "data-type": data_type,
                    "format": "climber-standardized",
                    "upload-timestamp": datetime.utcnow().isoformat(),
                },
            )

            url = f"s3://{bucket_name}/{key}"
            logger.info(
                "Successfully uploaded processed data",
                hotel_code=hotel_code,
                data_type=data_type,
                key=key,
                url=url,
            )

            return {"key": key, "url": url}

        except ClientError as e:
            logger.error(
                "Failed to upload processed data to S3",
                hotel_code=hotel_code,
                data_type=data_type,
                error=str(e),
            )
            raise S3UploadError(
                f"Failed to upload processed data for {hotel_code}/{data_type}: {str(e)}"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error uploading processed data",
                hotel_code=hotel_code,
                data_type=data_type,
                error=str(e),
            )
            raise S3UploadError(
                f"Unexpected error uploading processed data: {str(e)}"
            ) from e

    def get_object(self, bucket_name: str, key: str) -> str:
        """Retrieve object from S3.

        Args:
            bucket_name: S3 bucket name
            key: Object key

        Returns:
            Object content as string

        Raises:
            S3UploadError: If retrieval fails
        """
        logger.info(
            "Retrieving object from S3",
            bucket_name=bucket_name,
            key=key,
        )

        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response["Body"].read().decode("utf-8")

            logger.info(
                "Successfully retrieved object from S3",
                bucket_name=bucket_name,
                key=key,
            )

            return content

        except ClientError as e:
            logger.error(
                "Failed to retrieve object from S3",
                bucket_name=bucket_name,
                key=key,
                error=str(e),
            )
            raise S3UploadError(
                f"Failed to retrieve object from S3: {str(e)}"
            ) from e

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
    ) -> list[dict[str, Any]]:
        """List objects in S3 bucket with optional prefix.

        Args:
            bucket_name: S3 bucket name
            prefix: Optional object key prefix

        Returns:
            List of object metadata dictionaries

        Raises:
            S3UploadError: If listing fails
        """
        logger.info(
            "Listing objects in S3 bucket",
            bucket_name=bucket_name,
            prefix=prefix,
        )

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            objects = []
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        objects.append(
                            {
                                "key": obj["Key"],
                                "size": obj["Size"],
                                "last_modified": obj["LastModified"].isoformat(),
                            }
                        )

            logger.info(
                "Successfully listed objects",
                bucket_name=bucket_name,
                prefix=prefix,
                object_count=len(objects),
            )

            return objects

        except ClientError as e:
            logger.error(
                "Failed to list objects in S3",
                bucket_name=bucket_name,
                prefix=prefix,
                error=str(e),
            )
            raise S3UploadError(
                f"Failed to list objects in S3: {str(e)}"
            ) from e

    def delete_object(self, bucket_name: str, key: str) -> None:
        """Delete object from S3.

        Args:
            bucket_name: S3 bucket name
            key: Object key

        Raises:
            S3UploadError: If deletion fails
        """
        logger.info(
            "Deleting object from S3",
            bucket_name=bucket_name,
            key=key,
        )

        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=key)

            logger.info(
                "Successfully deleted object from S3",
                bucket_name=bucket_name,
                key=key,
            )

        except ClientError as e:
            logger.error(
                "Failed to delete object from S3",
                bucket_name=bucket_name,
                key=key,
                error=str(e),
            )
            raise S3UploadError(
                f"Failed to delete object from S3: {str(e)}"
            ) from e

    # ---- Climber padrão: single timestamp, explicit buckets ----

    def _timestamp_iso_seconds(self, timestamp: str | None = None) -> str:
        """Format timestamp for S3 key: ISO up to seconds (e.g. 2024-07-04T11:26:32Z)."""
        if timestamp:
            if "T" in timestamp and "Z" in timestamp:
                return timestamp[:19] + "Z" if len(timestamp) > 19 else timestamp
            return timestamp
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    def upload_raw_reservations(
        self,
        raw_data: Any,
        timestamp: str,
        hotel_code_s3: str,
        bucket: str | None = None,
    ) -> dict[str, str]:
        """Upload raw reservation payload to padrão raw bucket.

        Path: {hotel_code_s3}/reservations-{timestamp}.json

        Args:
            raw_data: Raw API response (dict or list).
            timestamp: ISO timestamp up to seconds (e.g. 2024-07-04T11:26:32Z).
            hotel_code_s3: Hotel code for S3 paths.
            bucket: Override bucket; if None, uses settings.padrao_raw_bucket().

        Returns:
            Dict with 'key' and 'url'.
        """
        ts = self._timestamp_iso_seconds(timestamp)
        key = f"{hotel_code_s3}/reservations-{ts}.json"
        bucket_name = bucket or settings.padrao_raw_bucket()
        if not bucket_name:
            bucket_name = settings.aws_s3_raw_prefix + "reservations"
        logger.info(
            "Uploading raw reservations (padrão)",
            hotel_code_s3=hotel_code_s3,
            key=key,
        )
        body = self._serialize_data(raw_data)
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "hotel-code-s3": hotel_code_s3,
                    "upload-timestamp": datetime.utcnow().isoformat(),
                },
            )
            url = f"s3://{bucket_name}/{key}"
            logger.info("Uploaded raw reservations", key=key, url=url)
            return {"key": key, "url": url}
        except ClientError as e:
            logger.error("Failed to upload raw reservations", key=key, error=str(e))
            raise S3UploadError(f"Upload raw reservations failed: {str(e)}") from e

    def upload_reservations(
        self,
        reservations: Any,
        timestamp: str,
        hotel_code_s3: str,
        bucket: str | None = None,
    ) -> dict[str, str]:
        """Upload transformed reservations to padrão reservations bucket.

        Path: {hotel_code_s3}/reservations-{timestamp}.json

        Args:
            reservations: List of reservation dicts or ReservationCollection.
            timestamp: Same ISO timestamp as raw/segments.
            hotel_code_s3: Hotel code for S3 paths.
            bucket: Override bucket; if None, uses settings.padrao_reservations_bucket().

        Returns:
            Dict with 'key' and 'url'.
        """
        ts = self._timestamp_iso_seconds(timestamp)
        key = f"{hotel_code_s3}/reservations-{ts}.json"
        bucket_name = bucket or settings.padrao_reservations_bucket()
        if not bucket_name:
            bucket_name = settings.aws_s3_processed_prefix + "reservations"
        if hasattr(reservations, "reservations"):
            body = json.dumps(
                [r.model_dump(by_alias=True) for r in reservations.reservations],
                indent=2,
                default=str,
            )
        else:
            body = self._serialize_data(reservations)
        logger.info(
            "Uploading reservations (padrão)",
            hotel_code_s3=hotel_code_s3,
            key=key,
        )
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "hotel-code-s3": hotel_code_s3,
                    "format": "climber-standardized",
                },
            )
            url = f"s3://{bucket_name}/{key}"
            logger.info("Uploaded reservations", key=key, url=url)
            return {"key": key, "url": url}
        except ClientError as e:
            logger.error("Failed to upload reservations", key=key, error=str(e))
            raise S3UploadError(f"Upload reservations failed: {str(e)}") from e

    def upload_segments(
        self,
        segments: Any,
        timestamp: str,
        hotel_code_s3: str,
        bucket: str | None = None,
    ) -> dict[str, str]:
        """Upload transformed segments to padrão segments bucket.

        Path: {hotel_code_s3}/segments-{timestamp}.json

        Args:
            segments: SegmentCollection or dict.
            timestamp: Same ISO timestamp as raw/reservations.
            hotel_code_s3: Hotel code for S3 paths.
            bucket: Override bucket; if None, uses settings.padrao_segments_bucket().

        Returns:
            Dict with 'key' and 'url'.
        """
        ts = self._timestamp_iso_seconds(timestamp)
        key = f"{hotel_code_s3}/segments-{ts}.json"
        bucket_name = bucket or settings.padrao_segments_bucket()
        if not bucket_name:
            bucket_name = settings.aws_s3_processed_prefix + "segments"
        body = self._serialize_data(segments)
        logger.info(
            "Uploading segments (padrão)",
            hotel_code_s3=hotel_code_s3,
            key=key,
        )
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "hotel-code-s3": hotel_code_s3,
                    "format": "climber-standardized",
                },
            )
            url = f"s3://{bucket_name}/{key}"
            logger.info("Uploaded segments", key=key, url=url)
            return {"key": key, "url": url}
        except ClientError as e:
            logger.error("Failed to upload segments", key=key, error=str(e))
            raise S3UploadError(f"Upload segments failed: {str(e)}") from e
