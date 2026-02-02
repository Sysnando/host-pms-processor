"""AWS SQS Manager for FIFO queue operations."""

import json
import uuid
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel
from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class SQSError(Exception):
    """Raised when SQS operation fails."""

    pass


class SQSManager:
    """Manages FIFO SQS queue operations for PMS processor triggers."""

    def __init__(self):
        """Initialize SQS Manager with AWS settings."""
        self.region = settings.aws.region
        self.sqs_client = boto3.client("sqs", region_name=self.region)
        self.queue_name = settings.aws.sqs_queue_name
        self._queue_url: Optional[str] = None

        logger.info("SQS Manager initialized", queue_name=self.queue_name)

    @property
    def queue_url(self) -> str:
        """Get queue URL, fetching it if not already cached."""
        if self._queue_url is None:
            try:
                response = self.sqs_client.get_queue_url(QueueName=self.queue_name)
                self._queue_url = response["QueueUrl"]
                logger.info(
                    "Successfully fetched SQS queue URL",
                    queue_name=self.queue_name,
                    queue_url=self._queue_url,
                )
            except ClientError as e:
                logger.error(
                    "Failed to fetch queue URL",
                    queue_name=self.queue_name,
                    error=str(e),
                )
                raise SQSError(
                    f"Failed to get SQS queue URL for {self.queue_name}: {str(e)}"
                ) from e
        return self._queue_url

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

    def send_message(
        self,
        hotel_code: str,
        file_type: str,
        file_key: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, str]:
        """Send a message to the SQS FIFO queue.

        FIFO queue ensures messages are processed in order per hotel.
        The GroupId is set to hotel_code to ensure hotel-specific ordering.

        Args:
            hotel_code: Hotel code (used as GroupId for FIFO ordering)
            file_type: Type of file (config, reservation, inventory, revenue)
            file_key: S3 key of the processed file
            metadata: Optional additional metadata

        Returns:
            Dictionary with 'message_id' of sent message

        Raises:
            SQSError: If message send fails
        """
        logger.info(
            "Sending message to SQS queue",
            hotel_code=hotel_code,
            file_type=file_type,
            queue_name=self.queue_name,
        )

        try:
            # Create message body
            message_body = {
                "hotelCode": hotel_code,
                "fileType": file_type,
                "fileKey": file_key,
            }

            # Add optional metadata
            if metadata:
                message_body.update(metadata)

            # Generate deduplication ID to prevent duplicate messages
            deduplication_id = str(
                uuid.uuid5(uuid.NAMESPACE_DNS, f"{hotel_code}{file_key}")
            )

            # Send to FIFO queue
            response = self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=self._serialize_message(message_body),
                GroupId=hotel_code,  # FIFO GroupId for ordering
                MessageDeduplicationId=deduplication_id,
                MessageAttributes={
                    "HotelCode": {"StringValue": hotel_code, "DataType": "String"},
                    "FileType": {"StringValue": file_type, "DataType": "String"},
                },
            )

            message_id = response["MessageId"]
            logger.info(
                "Successfully sent message to SQS queue",
                hotel_code=hotel_code,
                file_type=file_type,
                message_id=message_id,
                queue_name=self.queue_name,
            )

            return {"message_id": message_id}

        except ClientError as e:
            logger.error(
                "Failed to send message to SQS queue",
                hotel_code=hotel_code,
                file_type=file_type,
                queue_name=self.queue_name,
                error=str(e),
            )
            raise SQSError(
                f"Failed to send SQS message for {hotel_code}/{file_type}: {str(e)}"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error sending SQS message",
                hotel_code=hotel_code,
                file_type=file_type,
                error=str(e),
            )
            raise SQSError(
                f"Unexpected error sending SQS message: {str(e)}"
            ) from e

    def send_batch(
        self,
        messages: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Send multiple messages to the SQS FIFO queue.

        Args:
            messages: List of message dicts with keys: hotel_code, file_type, file_key

        Returns:
            Dictionary with 'successful' and 'failed' message counts

        Raises:
            SQSError: If batch send fails
        """
        logger.info(
            "Sending batch messages to SQS queue",
            message_count=len(messages),
            queue_name=self.queue_name,
        )

        try:
            successful = 0
            failed = 0

            for message in messages:
                try:
                    self.send_message(
                        hotel_code=message["hotel_code"],
                        file_type=message["file_type"],
                        file_key=message["file_key"],
                        metadata=message.get("metadata"),
                    )
                    successful += 1
                except SQSError as e:
                    logger.warning(
                        "Failed to send message in batch",
                        hotel_code=message.get("hotel_code"),
                        error=str(e),
                    )
                    failed += 1
                    continue

            logger.info(
                "Batch send complete",
                total_messages=len(messages),
                successful=successful,
                failed=failed,
            )

            return {"successful": successful, "failed": failed}

        except Exception as e:
            logger.error(
                "Unexpected error in batch send",
                error=str(e),
            )
            raise SQSError(f"Unexpected error in batch send: {str(e)}") from e

    def receive_messages(
        self,
        max_messages: int = 10,
        wait_time: int = 20,
    ) -> list[dict[str, Any]]:
        """Receive messages from the SQS FIFO queue.

        Args:
            max_messages: Maximum number of messages to receive (1-10)
            wait_time: Long polling wait time in seconds (0-20)

        Returns:
            List of message dictionaries

        Raises:
            SQSError: If receive fails
        """
        logger.info(
            "Receiving messages from SQS queue",
            queue_name=self.queue_name,
            max_messages=max_messages,
        )

        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=min(wait_time, 20),
                MessageAttributeNames=["All"],
            )

            messages = []
            if "Messages" in response:
                for message in response["Messages"]:
                    messages.append(
                        {
                            "message_id": message["MessageId"],
                            "receipt_handle": message["ReceiptHandle"],
                            "body": json.loads(message["Body"]),
                            "attributes": message.get("MessageAttributes", {}),
                        }
                    )

            logger.info(
                "Successfully received messages",
                queue_name=self.queue_name,
                message_count=len(messages),
            )

            return messages

        except ClientError as e:
            logger.error(
                "Failed to receive messages from SQS",
                queue_name=self.queue_name,
                error=str(e),
            )
            raise SQSError(f"Failed to receive SQS messages: {str(e)}") from e

    def delete_message(self, receipt_handle: str) -> None:
        """Delete a message from the SQS queue.

        Args:
            receipt_handle: Receipt handle from the message

        Raises:
            SQSError: If deletion fails
        """
        logger.info(
            "Deleting message from SQS queue",
            queue_name=self.queue_name,
        )

        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )

            logger.info(
                "Successfully deleted message from SQS queue",
                queue_name=self.queue_name,
            )

        except ClientError as e:
            logger.error(
                "Failed to delete message from SQS",
                queue_name=self.queue_name,
                error=str(e),
            )
            raise SQSError(f"Failed to delete SQS message: {str(e)}") from e

    def get_queue_attributes(self) -> dict[str, str]:
        """Get attributes of the SQS queue.

        Args:
            None

        Returns:
            Dictionary of queue attributes

        Raises:
            SQSError: If retrieval fails
        """
        logger.info(
            "Getting queue attributes",
            queue_name=self.queue_name,
        )

        try:
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["All"],
            )

            logger.info(
                "Successfully retrieved queue attributes",
                queue_name=self.queue_name,
            )

            return response["Attributes"]

        except ClientError as e:
            logger.error(
                "Failed to get queue attributes",
                queue_name=self.queue_name,
                error=str(e),
            )
            raise SQSError(f"Failed to get queue attributes: {str(e)}") from e
