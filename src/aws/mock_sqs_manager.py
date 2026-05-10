"""Mock SQS Manager for local testing without AWS infrastructure."""

import json
import uuid
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel
from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class MockSQSManager:
    """Mock SQSManager that logs messages instead of sending to SQS.

    This allows testing the full pipeline without AWS credentials or SQS access.
    Messages are logged and optionally saved to a local file.
    """

    def __init__(self, output_dir: str = "./data_extracts"):
        """Initialize mock SQS manager.

        Args:
            output_dir: Local directory to save message log (default: ./data_extract)
        """
        self.output_dir = Path(output_dir)
        self.queue_name = settings.aws.sqs_queue_name
        self.messages_log: list[dict[str, Any]] = []
        self.current_hotel_dir: Path | None = None  # Track current hotel directory

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "MockSQSManager initialized",
            queue_name=self.queue_name,
            output_dir=str(self.output_dir.absolute()),
        )

    def _serialize_message(self, data: Any) -> str:
        """Serialize message data to JSON string.

        Args:
            data: Message data (dict or Pydantic model)

        Returns:
            JSON string
        """
        if isinstance(data, BaseModel):
            return data.model_dump_json(by_alias=True)
        elif isinstance(data, dict):
            return json.dumps(data)
        else:
            return json.dumps({"data": str(data)})

    def _log_message(self, message: dict[str, Any]) -> None:
        """Log message to internal list and save to file.

        Args:
            message: Message dictionary to log
        """
        self.messages_log.append(message)

        # Determine where to save the file
        # If we have a current hotel directory, save there
        # Otherwise save to root output directory
        if self.current_hotel_dir and self.current_hotel_dir.exists():
            messages_file = self.current_hotel_dir / "sqs_messages.json"
        else:
            messages_file = self.output_dir / "sqs_messages.json"

        messages_file.write_text(
            json.dumps(self.messages_log, indent=2, default=str),
            encoding="utf-8",
        )

    def set_hotel_directory(self, hotel_dir: Path) -> None:
        """Set the current hotel directory for saving SQS messages.

        Args:
            hotel_dir: Path to the hotel's output directory
        """
        self.current_hotel_dir = hotel_dir
        # Reset messages log for new hotel
        self.messages_log = []

    def send_processor_message(
        self,
        hotel_code_s3: str,
        message_group_id: str | None = None,
        queue_url: str | None = None,
    ) -> dict[str, str]:
        """Log mock processor message instead of sending to SQS.

        Args:
            hotel_code_s3: Message body (hotel code for S3/ESB).
            message_group_id: MessageGroupId for FIFO (default from settings).
            queue_url: Override queue URL if provided.

        Returns:
            Dict with mock 'message_id'.
        """
        group_id = message_group_id or settings.padrao_sqs_message_group_id() or hotel_code_s3
        if not group_id or group_id.startswith("#") or " " in group_id:
            group_id = hotel_code_s3

        logger.info(
            "MOCK: Would send processor message (padrão)",
            hotel_code_s3=hotel_code_s3,
            message_group_id=group_id,
            queue_name=self.queue_name,
        )

        # Generate mock message ID
        message_id = str(uuid.uuid4())

        # Log message
        message = {
            "message_id": message_id,
            "message_type": "processor_message",
            "hotel_code_s3": hotel_code_s3,
            "message_group_id": group_id,
            "queue_name": self.queue_name,
            "queue_url": queue_url or "mock-queue-url",
        }
        self._log_message(message)

        logger.info(
            "MOCK: Processor message logged successfully",
            hotel_code_s3=hotel_code_s3,
            message_id=message_id,
        )

        return {"message_id": message_id}

    def send_message(
        self,
        hotel_code: str,
        file_type: str,
        file_key: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, str]:
        """Log mock message instead of sending to SQS.

        Args:
            hotel_code: Hotel code (used as GroupId for FIFO ordering)
            file_type: Type of file (config, reservation, inventory, revenue)
            file_key: S3 key of the processed file
            metadata: Optional additional metadata

        Returns:
            Dictionary with mock 'message_id' of sent message
        """
        logger.info(
            "MOCK: Would send message to SQS queue",
            hotel_code=hotel_code,
            file_type=file_type,
            file_key=file_key,
            queue_name=self.queue_name,
        )

        # Create message body
        message_body = {
            "hotelCode": hotel_code,
            "fileType": file_type,
            "fileKey": file_key,
        }

        # Add optional metadata
        if metadata:
            message_body.update(metadata)

        # Generate mock message ID
        message_id = str(uuid.uuid4())

        # Generate deduplication ID (same as real implementation)
        deduplication_id = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, f"{hotel_code}{file_key}")
        )

        # Log message
        message = {
            "message_id": message_id,
            "message_type": "standard_message",
            "group_id": hotel_code,
            "deduplication_id": deduplication_id,
            "message_body": message_body,
            "queue_name": self.queue_name,
            "message_attributes": {
                "HotelCode": {"StringValue": hotel_code, "DataType": "String"},
                "FileType": {"StringValue": file_type, "DataType": "String"},
            },
        }
        self._log_message(message)

        logger.info(
            "MOCK: Message logged successfully",
            hotel_code=hotel_code,
            file_type=file_type,
            message_id=message_id,
            queue_name=self.queue_name,
        )

        return {"message_id": message_id}

    def send_batch(
        self,
        messages: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Log mock batch messages instead of sending to SQS.

        Args:
            messages: List of message dicts with keys: hotel_code, file_type, file_key

        Returns:
            Dictionary with 'successful' and 'failed' message counts
        """
        logger.info(
            "MOCK: Would send batch messages to SQS queue",
            message_count=len(messages),
            queue_name=self.queue_name,
        )

        successful = 0

        for message in messages:
            try:
                self.send_message(
                    hotel_code=message["hotel_code"],
                    file_type=message["file_type"],
                    file_key=message["file_key"],
                    metadata=message.get("metadata"),
                )
                successful += 1
            except Exception as e:
                logger.warning(
                    "MOCK: Failed to log message in batch",
                    hotel_code=message.get("hotel_code"),
                    error=str(e),
                )
                continue

        logger.info(
            "MOCK: Batch logging complete",
            total_messages=len(messages),
            successful=successful,
        )

        return {"successful": successful, "failed": 0}

    def receive_messages(
        self,
        max_messages: int = 10,
        wait_time: int = 20,
    ) -> list[dict[str, Any]]:
        """Mock receive messages - returns empty list.

        Args:
            max_messages: Maximum number of messages to receive (1-10)
            wait_time: Long polling wait time in seconds (0-20)

        Returns:
            Empty list (no messages in mock)
        """
        logger.info(
            "MOCK: Would receive messages from SQS queue",
            queue_name=self.queue_name,
            max_messages=max_messages,
        )

        logger.info(
            "MOCK: No messages to receive (mock queue)",
            queue_name=self.queue_name,
        )

        return []

    def delete_message(self, receipt_handle: str) -> None:
        """Mock delete message - logs only.

        Args:
            receipt_handle: Receipt handle from the message
        """
        logger.info(
            "MOCK: Would delete message from SQS queue",
            queue_name=self.queue_name,
            receipt_handle=receipt_handle,
        )

        logger.info(
            "MOCK: Message deletion logged",
            queue_name=self.queue_name,
        )

    def get_queue_attributes(self) -> dict[str, str]:
        """Mock get queue attributes - returns mock attributes.

        Returns:
            Dictionary of mock queue attributes
        """
        logger.info(
            "MOCK: Would get queue attributes",
            queue_name=self.queue_name,
        )

        attributes = {
            "QueueArn": f"arn:aws:sqs:us-east-1:123456789012:{self.queue_name}",
            "ApproximateNumberOfMessages": "0",
            "ApproximateNumberOfMessagesNotVisible": "0",
            "ApproximateNumberOfMessagesDelayed": "0",
            "CreatedTimestamp": "1234567890",
            "LastModifiedTimestamp": "1234567890",
            "VisibilityTimeout": "30",
            "MaximumMessageSize": "262144",
            "MessageRetentionPeriod": "345600",
            "DelaySeconds": "0",
            "ReceiveMessageWaitTimeSeconds": "20",
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        }

        logger.info(
            "MOCK: Returning mock queue attributes",
            queue_name=self.queue_name,
        )

        return attributes
