"""Mock S3 Manager for local testing without AWS infrastructure."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from structlog import get_logger

logger = get_logger(__name__)


class MockS3Manager:
    """Mock S3Manager that logs and saves files locally instead of uploading to S3.

    This allows testing the full pipeline without AWS credentials or S3 access.
    Files are saved to a local directory for inspection.
    """

    def __init__(self, output_dir: str = "./data_extracts"):
        """Initialize mock S3 manager with local output directory.

        Args:
            output_dir: Local directory to save files (default: ./data_extract)
        """
        self.output_dir = Path(output_dir)
        self.raw_prefix = "mock-raw-"
        self.processed_prefix = "mock-processed-"
        self.timestamp = datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.current_hotel_dir: Path | None = None  # Track current hotel directory

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "MockS3Manager initialized",
            output_dir=str(self.output_dir.absolute()),
        )

    def get_hotel_directory(self, hotel_code: str) -> Path:
        """Get the hotel-specific directory path.

        Args:
            hotel_code: Hotel code

        Returns:
            Path to hotel directory
        """
        hotel_timestamp_dir = f"{hotel_code}_{self.timestamp}"
        return self.output_dir / hotel_timestamp_dir

    def _get_bucket_name(self, prefix: str, data_type: str) -> str:
        """Construct mock S3 bucket name from prefix and data type.

        Args:
            prefix: Base prefix (raw or processed)
            data_type: Type of data (hotel-configs, reservations, etc.)

        Returns:
            Mock S3 bucket name
        """
        return f"{prefix}{data_type}"

    def _serialize_data(self, data: Any) -> str:
        """Serialize data to JSON string.

        Handles both Pydantic models, dicts, and lists of Pydantic models.

        Args:
            data: Data to serialize (dict, Pydantic model, or list)

        Returns:
            JSON string
        """
        if isinstance(data, BaseModel):
            return data.model_dump_json(by_alias=True)
        elif isinstance(data, list):
            # Check if list contains Pydantic models
            if data and isinstance(data[0], BaseModel):
                # Convert list of Pydantic models to list of dicts
                serialized_list = [item.model_dump(by_alias=True) for item in data]
                return json.dumps(serialized_list, default=str)
            else:
                return json.dumps(data, default=str)
        elif isinstance(data, dict):
            return json.dumps(data, default=str)
        else:
            return json.dumps({"data": str(data)})

    def _save_file(
        self,
        hotel_code: str,
        bucket_name: str,
        key: str,
        content: str,
    ) -> Path:
        """Save file to local directory.

        Args:
            hotel_code: Hotel code
            bucket_name: Mock bucket name
            key: File key
            content: File content

        Returns:
            Path to saved file
        """
        # Create directory structure: output_dir/hotel_code_timestamp/
        hotel_timestamp_dir = f"{hotel_code}_{self.timestamp}"
        output_dir = self.output_dir / hotel_timestamp_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        # Extract filename from key and add bucket prefix
        filename = Path(key).name
        # Add bucket name as prefix to distinguish raw vs processed
        bucket_type = "raw" if "raw" in bucket_name else "processed"
        data_type = bucket_name.replace("mock-raw-", "").replace("mock-processed-", "")
        prefixed_filename = f"{bucket_type}_{data_type}_{filename}"

        file_path = output_dir / prefixed_filename

        # Write file
        file_path.write_text(content, encoding="utf-8")

        return file_path

    def upload_raw(
        self,
        hotel_code: str,
        data_type: str,
        data: Any,
        custom_suffix: str = "",
    ) -> dict[str, str]:
        """Mock upload raw data - logs and saves file locally.

        Args:
            hotel_code: Hotel code identifier
            data_type: Type of data (hotel-configs, reservations, inventory, revenue)
            data: Data to upload (dict or Pydantic model)
            custom_suffix: Optional custom suffix for filename

        Returns:
            Dictionary with 'key' and 'url' of mock uploaded file
        """
        logger.info(
            "MOCK: Would upload raw data to S3",
            hotel_code=hotel_code,
            data_type=data_type,
        )

        bucket_name = self._get_bucket_name(self.raw_prefix, data_type)
        suffix = custom_suffix or self.timestamp
        key = f"{hotel_code}/{data_type}-{suffix}.json"

        # Serialize data
        body = self._serialize_data(data)

        # Save file locally
        file_path = self._save_file(hotel_code, bucket_name, key, body)

        url = f"s3://{bucket_name}/{key}"

        logger.info(
            "MOCK: Successfully saved raw data locally",
            hotel_code=hotel_code,
            data_type=data_type,
            key=key,
            url=url,
            local_path=str(file_path.absolute()),
            file_size_bytes=len(body),
        )

        return {"key": key, "url": url}

    def upload_processed(
        self,
        hotel_code: str,
        data_type: str,
        data: Any,
        custom_suffix: str = "",
    ) -> dict[str, str]:
        """Mock upload processed data - logs and saves file locally.

        Args:
            hotel_code: Hotel code identifier
            data_type: Type of data (hotel-configs, reservations, inventory, revenue)
            data: Data to upload (dict or Pydantic model)
            custom_suffix: Optional custom suffix for filename

        Returns:
            Dictionary with 'key' and 'url' of mock uploaded file
        """
        logger.info(
            "MOCK: Would upload processed data to S3",
            hotel_code=hotel_code,
            data_type=data_type,
        )

        bucket_name = self._get_bucket_name(self.processed_prefix, data_type)
        suffix = custom_suffix or self.timestamp
        key = f"{hotel_code}/{data_type}-{suffix}.json"

        # Serialize data
        body = self._serialize_data(data)

        # Save file locally
        file_path = self._save_file(hotel_code, bucket_name, key, body)

        url = f"s3://{bucket_name}/{key}"

        logger.info(
            "MOCK: Successfully saved processed data locally",
            hotel_code=hotel_code,
            data_type=data_type,
            key=key,
            url=url,
            local_path=str(file_path.absolute()),
            file_size_bytes=len(body),
        )

        return {"key": key, "url": url}

    def get_object(self, bucket_name: str, key: str) -> str:
        """Mock retrieve object from S3 - reads from local directory.

        Args:
            bucket_name: S3 bucket name
            key: Object key

        Returns:
            Object content as string

        Raises:
            FileNotFoundError: If file doesn't exist locally
        """
        logger.info(
            "MOCK: Would retrieve object from S3",
            bucket_name=bucket_name,
            key=key,
        )

        # Extract hotel_code and filename from key
        parts = key.split("/")
        if len(parts) >= 2:
            hotel_code = parts[0]
            filename = parts[-1]
            file_path = self.output_dir / bucket_name / hotel_code / filename

            if file_path.exists():
                content = file_path.read_text(encoding="utf-8")
                logger.info(
                    "MOCK: Successfully retrieved object from local directory",
                    bucket_name=bucket_name,
                    key=key,
                    local_path=str(file_path.absolute()),
                )
                return content

        raise FileNotFoundError(f"Mock file not found: {bucket_name}/{key}")

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
    ) -> list[dict[str, Any]]:
        """Mock list objects in S3 bucket - lists local files.

        Args:
            bucket_name: S3 bucket name
            prefix: Optional object key prefix

        Returns:
            List of object metadata dictionaries
        """
        logger.info(
            "MOCK: Would list objects in S3 bucket",
            bucket_name=bucket_name,
            prefix=prefix,
        )

        bucket_dir = self.output_dir / bucket_name

        if not bucket_dir.exists():
            logger.info(
                "MOCK: Bucket directory does not exist",
                bucket_name=bucket_name,
            )
            return []

        objects = []
        for file_path in bucket_dir.rglob("*.json"):
            # Construct key from relative path
            relative_path = file_path.relative_to(bucket_dir)
            key = str(relative_path).replace("\\", "/")

            if prefix and not key.startswith(prefix):
                continue

            stat = file_path.stat()
            objects.append({
                "key": key,
                "size": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })

        logger.info(
            "MOCK: Successfully listed objects",
            bucket_name=bucket_name,
            prefix=prefix,
            object_count=len(objects),
        )

        return objects

    def delete_object(self, bucket_name: str, key: str) -> None:
        """Mock delete object from S3 - deletes local file.

        Args:
            bucket_name: S3 bucket name
            key: Object key
        """
        logger.info(
            "MOCK: Would delete object from S3",
            bucket_name=bucket_name,
            key=key,
        )

        # Extract hotel_code and filename from key
        parts = key.split("/")
        if len(parts) >= 2:
            hotel_code = parts[0]
            filename = parts[-1]
            file_path = self.output_dir / bucket_name / hotel_code / filename

            if file_path.exists():
                file_path.unlink()
                logger.info(
                    "MOCK: Successfully deleted object from local directory",
                    bucket_name=bucket_name,
                    key=key,
                    local_path=str(file_path.absolute()),
                )
            else:
                logger.warning(
                    "MOCK: Object not found for deletion",
                    bucket_name=bucket_name,
                    key=key,
                )
