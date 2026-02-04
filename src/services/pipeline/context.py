"""Pipeline context for sharing data between steps."""

from datetime import datetime
from typing import Any


class PipelineContext:
    """Context object for passing data between pipeline steps.

    This object is passed to each step and accumulates results
    as the pipeline progresses.
    """

    def __init__(self, hotel_code: str):
        """Initialize pipeline context.

        Args:
            hotel_code: Hotel code being processed
        """
        self.hotel_code = hotel_code
        self.start_time = datetime.utcnow()

        # Input parameters
        self.last_import_date: str | None = None

        # Raw API responses
        self.config_response: dict[str, Any] | None = None
        self.reservations_response: dict[str, Any] | None = None
        self.stat_daily_records: list[dict[str, Any]] = []

        # Transformed data
        self.config_data: Any = None
        self.segments_collection: Any = None
        self.room_inventory: Any = None
        self.reservations_collection: Any = None

        # S3 upload results
        self.s3_uploads: dict[str, dict[str, str]] = {}

        # SQS messages to send
        self.sqs_messages: list[dict[str, str]] = []

        # Processing statistics
        self.stats: dict[str, Any] = {}

        # Errors encountered during processing
        self.errors: list[dict[str, str]] = []

        # Success flag
        self.success: bool = False

    def add_error(self, step_name: str, error_message: str) -> None:
        """Add an error to the context.

        Args:
            step_name: Name of the step where error occurred
            error_message: Error message
        """
        self.errors.append({
            "step": step_name,
            "message": error_message,
            "timestamp": datetime.utcnow().isoformat(),
        })

    def add_s3_upload(self, data_type: str, upload_result: dict[str, str]) -> None:
        """Record an S3 upload result.

        Args:
            data_type: Type of data uploaded (e.g., 'config', 'reservations')
            upload_result: Upload result containing 'key' and 'url'
        """
        self.s3_uploads[data_type] = upload_result

    def add_sqs_message(self, file_type: str, file_key: str) -> None:
        """Add an SQS message to be sent.

        Args:
            file_type: Type of file (e.g., 'config', 'reservations')
            file_key: S3 key of the file
        """
        self.sqs_messages.append({
            "hotel_code": self.hotel_code,
            "file_type": file_type,
            "file_key": file_key,
        })

    def has_errors(self) -> bool:
        """Check if any errors were encountered.

        Returns:
            True if errors exist, False otherwise
        """
        return len(self.errors) > 0

    def get_results(self) -> dict[str, Any]:
        """Get final results dictionary.

        Returns:
            Dictionary containing all results and statistics
        """
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()

        return {
            "hotel_code": self.hotel_code,
            "success": self.success,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "errors": self.errors,
            "stats": self.stats,
            "s3_uploads": self.s3_uploads,
            "sqs_messages": self.sqs_messages,
        }
