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
        self.worker_id: int | None = None
        self.start_time = datetime.now(datetime.timezone.utc)

        # Input parameters from ESB
        self.last_import_date: str | None = None
        self.min_import_date: str | None = None
        self.max_import_date: str | None = None
        self.is_first_import: bool = False  # True if KpisRecordDateMax was null/empty

        # Calculated date ranges (based on ESB parameters)
        self.calculated_reservation_from_date: str | None = None
        self.calculated_stat_daily_start: str | None = None
        self.calculated_stat_daily_end: str | None = None
        self.calculated_inventory_from: str | None = None
        self.calculated_inventory_to: str | None = None

        # Raw API responses
        self.config_response: dict[str, Any] | None = None
        self.reservations_response: dict[str, Any] | None = None
        self.stat_daily_records: list[dict[str, Any]] = []

        # Hotel local time (extracted from config for ESB registration)
        self.hotel_local_time: datetime | None = None

        # Transformed data
        self.config_data: Any = None
        self.segments_collection: Any = None
        self.room_inventory: Any = None
        self.reservations_collection: Any = None

        # S3 upload results
        self.s3_uploads: dict[str, dict[str, str]] = {}

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
            "timestamp": datetime.now(datetime.timezone.utc).isoformat(),
        })

    def add_s3_upload(self, data_type: str, upload_result: dict[str, str]) -> None:
        """Record an S3 upload result.

        Args:
            data_type: Type of data uploaded (e.g., 'config', 'reservations')
            upload_result: Upload result containing 'key' and 'url'
        """
        self.s3_uploads[data_type] = upload_result

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
        end_time = datetime.now(datetime.timezone.utc)
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
        }
