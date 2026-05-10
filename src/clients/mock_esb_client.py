"""Mock Climber ESB API client for local testing without ESB infrastructure."""

from datetime import datetime, timedelta, timezone
from typing import Any

from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class MockClimberESBClient:
    """Mock ClimberESBClient that logs API calls instead of making real requests.

    This allows testing the full pipeline without Climber ESB access.
    Returns reasonable test data based on configuration.
    """

    def __init__(self):
        """Initialize the mock ESB client."""
        logger.info("MockClimberESBClient initialized")

    async def get_hotels(self) -> list[dict[str, Any]]:
        """Return mock list of configured hotels.

        If hotel_code is configured in settings, returns just that hotel.
        Otherwise returns a test list.

        Returns:
            List of hotel dictionaries with 'code' field
        """
        logger.info("MOCK: Would fetch hotel list from ESB")

        # If specific hotel is configured, return just that one
        hotel_code = (settings.hotel_code or settings.hotel.hotel_code or "").strip()

        if hotel_code:
            hotels = [{"code": hotel_code, "name": f"Test Hotel {hotel_code}"}]
        else:
            # Return test list of hotels
            hotels = [
                {"code": "HOTEL001", "name": "Test Hotel 001"},
                {"code": "HOTEL002", "name": "Test Hotel 002"},
            ]

        logger.info(
            "MOCK: Returning mock hotel list",
            hotel_count=len(hotels),
            hotels=[h["code"] for h in hotels],
        )

        return hotels

    async def get_hotel_parameters(self, hotel_code: str) -> dict[str, Any]:
        """Return mock import parameters for a hotel.

        Returns reasonable test parameters:
        - lastImportDate: 7 days ago
        - minImportDate: 30 days ago
        - maxImportDate: today

        Args:
            hotel_code: The hotel code identifier

        Returns:
            Dictionary containing import parameters
        """
        logger.info(
            "MOCK: Would fetch import parameters for hotel",
            hotel_code=hotel_code,
        )

        # Calculate test dates
        now = datetime.now(timezone.utc)
        last_import = now - timedelta(days=7)
        min_import = now - timedelta(days=30)
        max_import = now

        parameters = {
            "hotelCode": hotel_code,
            "lastImportDate": last_import.isoformat() + "Z",
            "minImportDate": min_import.isoformat() + "Z",
            "maxImportDate": max_import.isoformat() + "Z",
            "isFirstImport": False,  # Mock always returns existing hotel with import history
        }

        logger.info(
            "MOCK: Returning mock hotel parameters",
            hotel_code=hotel_code,
            last_import_date=parameters["lastImportDate"],
            is_first_import=False,
        )

        return parameters

    async def register_file(
        self,
        hotel_code: str,
        file_type: str,
        file_url: str,
        file_key: str,
        record_count: int,
        is_first_import: bool = False,
    ) -> dict[str, Any]:
        """Log mock file registration instead of calling ESB.

        Args:
            hotel_code: The hotel code
            file_type: Type of file (config, reservation, inventory, revenue)
            file_url: S3 URL or path to the processed file
            file_key: S3 object key for the processed file
            record_count: Number of records in the file
            is_first_import: If True, sets complete=True (when KpisRecordDateMax was null/empty)

        Returns:
            Mock registration response
        """
        logger.info(
            "MOCK: Would register file with ESB",
            hotel_code=hotel_code,
            file_type=file_type,
            file_url=file_url,
            file_key=file_key,
            record_count=record_count,
            is_first_import=is_first_import,
            complete=is_first_import,
        )

        response = {
            "success": True,
            "hotelCode": hotel_code,
            "fileType": file_type,
            "fileKey": file_key,
            "recordCount": record_count,
            "registeredAt": datetime.now(timezone.utc).isoformat() + "Z",
        }

        logger.info(
            "MOCK: File registration logged successfully",
            hotel_code=hotel_code,
            file_type=file_type,
        )

        return response

    async def update_import_date(
        self, hotel_code: str, last_import_date: str
    ) -> dict[str, Any]:
        """Log mock import date update instead of calling ESB.

        Args:
            hotel_code: The hotel code
            last_import_date: ISO format date/time string

        Returns:
            Mock update response
        """
        logger.info(
            "MOCK: Would update import date for hotel",
            hotel_code=hotel_code,
            last_import_date=last_import_date,
        )

        response = {
            "success": True,
            "hotelCode": hotel_code,
            "lastImportDate": last_import_date,
            "updatedAt": datetime.now(timezone.utc).isoformat() + "Z",
        }

        logger.info(
            "MOCK: Import date update logged successfully",
            hotel_code=hotel_code,
            last_import_date=last_import_date,
        )

        return response

    async def get_hotel_credentials(self, hotel_code: str) -> dict[str, str]:
        """Return mock credentials (not used in local testing).

        Args:
            hotel_code: The hotel code

        Returns:
            Dictionary with mock credentials
        """
        logger.info(
            "MOCK: Would fetch hotel credentials from ESB",
            hotel_code=hotel_code,
        )

        credentials = {
            "username": f"test_user_{hotel_code}",
            "password": "test_password",
        }

        logger.info(
            "MOCK: Returning mock credentials",
            hotel_code=hotel_code,
        )

        return credentials
