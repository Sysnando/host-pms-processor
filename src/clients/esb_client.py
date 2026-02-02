"""Climber ESB API client for hotel configuration and file registration."""

import asyncio
from typing import Any, Optional

import httpx
from structlog import get_logger

from src.clients.redis_token_manager import RedisTokenManager
from src.config import settings

logger = get_logger(__name__)


class ESBClientError(Exception):
    """Base exception for ESB client errors."""

    pass


class ESBAuthenticationError(ESBClientError):
    """Raised when ESB authentication fails."""

    pass


class ESBNotFoundError(ESBClientError):
    """Raised when ESB resource is not found."""

    pass


class ESBServerError(ESBClientError):
    """Raised when ESB returns a server error."""

    pass


class ClimberESBClient:
    """Client for Climber ESB API endpoints."""

    def __init__(self):
        """Initialize the ESB client with settings."""
        self.base_url = settings.esb.base_url.rstrip("/")
        self.timeout = settings.esb.request_timeout
        self.max_retries = settings.esb.max_retries
        self.retry_backoff_base = 2  # Exponential backoff base
        self.token_manager = RedisTokenManager()

    async def _get_headers(self) -> dict[str, str]:
        """Get default headers for ESB API requests with OAuth token from Redis.

        Returns:
            Dictionary of HTTP headers including authentication.
        """
        token = await self.token_manager.get_auth_token()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "HostPMSConnector/1.0",
        }

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Make an HTTP request to the ESB API with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint path (without base URL)
            data: Request body data (for POST/PUT requests)
            params: Query parameters

        Returns:
            JSON response as a dictionary

        Raises:
            ESBAuthenticationError: If authentication fails
            ESBNotFoundError: If resource not found
            ESBServerError: If server error occurs
            ESBClientError: For other API errors
        """
        url = f"{self.base_url}{endpoint}"
        headers = await self._get_headers()

        for attempt in range(self.max_retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                        params=params,
                    )

                    # Handle authentication errors
                    if response.status_code == 401:
                        logger.error(
                            "ESB authentication failed",
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise ESBAuthenticationError(
                            f"Authentication failed for {endpoint}: {response.text}"
                        )

                    # Handle not found errors
                    if response.status_code == 404:
                        logger.warning(
                            "ESB resource not found",
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise ESBNotFoundError(
                            f"Resource not found: {endpoint}"
                        )

                    # Handle server errors with retry
                    if response.status_code >= 500:
                        if attempt < self.max_retries - 1:
                            wait_time = self.retry_backoff_base ** attempt
                            logger.warning(
                                "ESB server error, retrying",
                                endpoint=endpoint,
                                status_code=response.status_code,
                                attempt=attempt + 1,
                                max_retries=self.max_retries,
                                wait_seconds=wait_time,
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                "ESB server error, max retries exceeded",
                                endpoint=endpoint,
                                status_code=response.status_code,
                            )
                            raise ESBServerError(
                                f"Server error at {endpoint}: {response.text}"
                            )

                    # Handle client errors (non-auth, non-404)
                    if 400 <= response.status_code < 500:
                        logger.error(
                            "ESB client error",
                            endpoint=endpoint,
                            status_code=response.status_code,
                            response_text=response.text,
                        )
                        raise ESBClientError(
                            f"Client error at {endpoint}: {response.text}"
                        )

                    # Handle success
                    if response.status_code in (200, 201, 204):
                        logger.debug(
                            "ESB request successful",
                            endpoint=endpoint,
                            method=method,
                            status_code=response.status_code,
                        )
                        if response.text:
                            return response.json()
                        return {}

                    # Unexpected status code
                    logger.error(
                        "Unexpected ESB response status",
                        endpoint=endpoint,
                        status_code=response.status_code,
                    )
                    raise ESBClientError(
                        f"Unexpected response from {endpoint}: {response.status_code}"
                    )

            except httpx.TimeoutException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "ESB request timeout, retrying",
                        endpoint=endpoint,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        wait_seconds=wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        "ESB request timeout, max retries exceeded",
                        endpoint=endpoint,
                    )
                    raise ESBClientError(f"Request timeout for {endpoint}") from e

            except httpx.RequestError as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "ESB request error, retrying",
                        endpoint=endpoint,
                        error=str(e),
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        wait_seconds=wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        "ESB request error, max retries exceeded",
                        endpoint=endpoint,
                        error=str(e),
                    )
                    raise ESBClientError(
                        f"Request failed for {endpoint}: {str(e)}"
                    ) from e

        raise ESBClientError(f"Failed to complete request to {endpoint}")

    async def get_hotels(self) -> list[dict[str, Any]]:
        """Fetch the list of configured hotels from ESB.

        Returns:
            List of hotel dictionaries with at least 'code' field

        Raises:
            ESBClientError: If the API request fails
        """
        logger.info("Fetching hotel list from ESB")
        response = await self._make_request("GET", "/hotels")
        hotels = response.get("hotels", response) if isinstance(response, dict) else response
        logger.info("Successfully fetched hotels from ESB", hotel_count=len(hotels))
        return hotels

    async def get_hotel_parameters(self, hotel_code: str) -> dict[str, Any]:
        """Fetch import parameters for a specific hotel.

        The parameters include:
        - lastImportDate: Last successful import date/time
        - minImportDate: Minimum allowed import date (optional)
        - maxImportDate: Maximum allowed import date (optional)

        Args:
            hotel_code: The hotel code identifier

        Returns:
            Dictionary containing import parameters

        Raises:
            ESBClientError: If the API request fails
            ESBNotFoundError: If the hotel is not found
        """
        logger.info("Fetching import parameters for hotel", hotel_code=hotel_code)
        response = await self._make_request(
            "GET", f"/hotels/{hotel_code}/parameters"
        )
        logger.info(
            "Successfully fetched hotel parameters",
            hotel_code=hotel_code,
            last_import_date=response.get("lastImportDate"),
        )
        return response

    async def register_file(
        self,
        hotel_code: str,
        file_type: str,
        file_url: str,
        file_key: str,
        record_count: int,
    ) -> dict[str, Any]:
        """Register an imported file with the ESB.

        Args:
            hotel_code: The hotel code
            file_type: Type of file (config, reservation, inventory, revenue)
            file_url: S3 URL or path to the processed file
            file_key: S3 object key for the processed file
            record_count: Number of records in the file

        Returns:
            Registration response from ESB

        Raises:
            ESBClientError: If the registration fails
        """
        logger.info(
            "Registering file with ESB",
            hotel_code=hotel_code,
            file_type=file_type,
            record_count=record_count,
        )
        payload = {
            "hotelCode": hotel_code,
            "fileType": file_type,
            "fileUrl": file_url,
            "fileKey": file_key,
            "recordCount": record_count,
        }
        response = await self._make_request(
            "POST", "/files/register", data=payload
        )
        logger.info(
            "Successfully registered file with ESB",
            hotel_code=hotel_code,
            file_type=file_type,
            file_key=file_key,
        )
        return response

    async def update_import_date(
        self, hotel_code: str, last_import_date: str
    ) -> dict[str, Any]:
        """Update the last import date for a hotel.

        Args:
            hotel_code: The hotel code
            last_import_date: ISO format date/time string (e.g., "2024-01-15T10:30:45Z")

        Returns:
            Update response from ESB

        Raises:
            ESBClientError: If the update fails
        """
        logger.info(
            "Updating import date for hotel",
            hotel_code=hotel_code,
            last_import_date=last_import_date,
        )
        payload = {"lastImportDate": last_import_date}
        response = await self._make_request(
            "PUT", f"/hotels/{hotel_code}/import-dates", data=payload
        )
        logger.info(
            "Successfully updated import date",
            hotel_code=hotel_code,
            last_import_date=last_import_date,
        )
        return response

    async def get_hotel_credentials(self, hotel_code: str) -> dict[str, str]:
        """Fetch Host PMS API credentials for a specific hotel.

        Returns:
            Dictionary with 'username' and 'password' or PMS-specific credentials

        Raises:
            ESBClientError: If the API request fails
            ESBNotFoundError: If the hotel is not found
        """
        logger.info("Fetching hotel credentials from ESB", hotel_code=hotel_code)
        response = await self._make_request(
            "GET", f"/hotels/{hotel_code}/credentials"
        )
        logger.info(
            "Successfully fetched hotel credentials",
            hotel_code=hotel_code,
        )
        return response
