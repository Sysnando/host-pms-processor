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
        # OAuth token endpoint uses :9443
        self.base_url = settings.esb.base_url.rstrip("/")
        # API endpoints use /pms-integration/1.0 (no port)
        # Extract hostname from base URL (e.g., "https://qa-esb.climberrms.com:9443" -> "qa-esb.climberrms.com")
        if "://" in self.base_url:
            # Extract hostname after protocol
            hostname = self.base_url.split("://")[1].split(":")[0]
        else:
            # No protocol, split by port
            hostname = self.base_url.split(":")[0]

        self.api_base_url = f"https://{hostname}/pms-integration/1.0"

        self.timeout = settings.esb.request_timeout
        self.max_retries = settings.esb.max_retries
        self.retry_backoff_base = 2  # Exponential backoff base
        self.token_manager = RedisTokenManager()

        logger.debug(
            "ESB client initialized",
            oauth_base_url=self.base_url,
            api_base_url=self.api_base_url,
        )

    async def _get_headers(self) -> dict[str, str]:
        """Get default headers for ESB API requests with OAuth token from Redis.

        Returns:
            Dictionary of HTTP headers including authentication.
        """
        token = await self.token_manager.get_auth_token()
        return {
            "Accept": "application/json",
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
        # Determine which base URL to use
        # If endpoint starts with /pms-integration, construct full API URL
        if endpoint.startswith("/pms-integration"):
            # Extract hostname from api_base_url
            if "://" in self.api_base_url:
                hostname = self.api_base_url.split("://")[1].split("/")[0]
            else:
                hostname = self.api_base_url.split("/")[0]
            url = f"https://{hostname}{endpoint}"
        else:
            url = f"{self.base_url}{endpoint}"

        # Track if we've already attempted token refresh for this request
        token_refreshed = False

        for attempt in range(self.max_retries):
            # Get fresh headers (important: do this inside the loop in case token was refreshed)
            headers = await self._get_headers()

            # Debug: Print request details
            print(f"\n{'=' * 80}")
            print("ESB REQUEST DEBUG")
            print(f"{'=' * 80}")
            print(f"Method: {method}")
            print(f"URL: {url}")
            if params:
                print(f"Query Params: {params}")
            print(f"Headers: {dict((k, v[:20] + '...' if k == 'Authorization' and len(v) > 20 else v) for k, v in headers.items())}")
            if data:
                print(f"Body: {data}")
            print(f"Attempt: {attempt + 1}/{self.max_retries}")
            if token_refreshed:
                print("Token was refreshed - retrying with fresh token")
            print(f"{'=' * 80}\n")
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                        params=params,
                    )

                    # Handle authentication errors with automatic token refresh
                    if response.status_code == 401:
                        # If we haven't tried refreshing the token yet, do it now
                        if not token_refreshed:
                            print(f"\n{'=' * 80}")
                            print("ESB REQUEST FAILED - AUTHENTICATION ERROR (401)")
                            print("Attempting automatic token refresh...")
                            print(f"{'=' * 80}")
                            print(f"Method: {method}")
                            print(f"URL: {url}")
                            if params:
                                print(f"Query Params: {params}")
                            print(f"Status Code: {response.status_code}")
                            print(f"Response: {response.text}")
                            print(f"Action: Clearing cached token and retrying with fresh token")
                            print(f"{'=' * 80}\n")

                            logger.warning(
                                "ESB authentication failed - clearing cached token and retrying",
                                endpoint=endpoint,
                                status_code=response.status_code,
                                url=url,
                            )

                            # Clear the cached token from Redis
                            await self.token_manager.clear_cache()

                            # Set flag to prevent infinite retry loop
                            token_refreshed = True

                            # Continue to next iteration (retry with fresh token)
                            continue
                        else:
                            # Token was already refreshed but still getting 401 - give up
                            print(f"\n{'=' * 80}")
                            print("ESB REQUEST FAILED - AUTHENTICATION ERROR (401) AFTER TOKEN REFRESH")
                            print(f"{'=' * 80}")
                            print(f"Method: {method}")
                            print(f"URL: {url}")
                            if params:
                                print(f"Query Params: {params}")
                            print(f"Status Code: {response.status_code}")
                            print(f"Response: {response.text}")
                            print(f"Note: Token was refreshed but authentication still failed")
                            print(f"{'=' * 80}\n")

                            logger.error(
                                "ESB authentication failed even after token refresh",
                                endpoint=endpoint,
                                status_code=response.status_code,
                                url=url,
                            )
                            raise ESBAuthenticationError(
                                f"Authentication failed for {endpoint} even after token refresh: {response.text}"
                            )

                    # Handle not found errors
                    if response.status_code == 404:
                        # Print full URL for debugging
                        print(f"\n{'=' * 80}")
                        print("ESB REQUEST FAILED - NOT FOUND (404)")
                        print(f"{'=' * 80}")
                        print(f"Method: {method}")
                        print(f"URL: {url}")
                        if params:
                            print(f"Query Params: {params}")
                        print(f"Status Code: {response.status_code}")
                        print(f"Response: {response.text}")
                        print(f"{'=' * 80}\n")

                        logger.warning(
                            "ESB resource not found",
                            endpoint=endpoint,
                            status_code=response.status_code,
                            url=url,
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
                            # Print full URL for debugging
                            print(f"\n{'=' * 80}")
                            print("ESB REQUEST FAILED - SERVER ERROR (5xx)")
                            print(f"{'=' * 80}")
                            print(f"Method: {method}")
                            print(f"URL: {url}")
                            if params:
                                print(f"Query Params: {params}")
                            print(f"Status Code: {response.status_code}")
                            print(f"Response: {response.text}")
                            print(f"Attempts: {attempt + 1}/{self.max_retries}")
                            print(f"{'=' * 80}\n")

                            logger.error(
                                "ESB server error, max retries exceeded",
                                endpoint=endpoint,
                                status_code=response.status_code,
                                url=url,
                            )
                            raise ESBServerError(
                                f"Server error at {endpoint}: {response.text}"
                            )

                    # Handle client errors (non-auth, non-404)
                    if 400 <= response.status_code < 500:
                        # Print full URL for debugging
                        print(f"\n{'=' * 80}")
                        print("ESB REQUEST FAILED - CLIENT ERROR (4xx)")
                        print(f"{'=' * 80}")
                        print(f"Method: {method}")
                        print(f"URL: {url}")
                        if params:
                            print(f"Query Params: {params}")
                        print(f"Status Code: {response.status_code}")
                        print(f"Response: {response.text}")
                        print(f"{'=' * 80}\n")

                        logger.error(
                            "ESB client error",
                            endpoint=endpoint,
                            status_code=response.status_code,
                            response_text=response.text,
                            url=url,
                        )
                        raise ESBClientError(
                            f"Client error at {endpoint}: {response.text}"
                        )

                    # Handle success
                    if response.status_code in (200, 201, 202, 204):
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
                    # Print full URL for debugging
                    print(f"\n{'=' * 80}")
                    print("ESB REQUEST FAILED - UNEXPECTED STATUS CODE")
                    print(f"{'=' * 80}")
                    print(f"Method: {method}")
                    print(f"URL: {url}")
                    if params:
                        print(f"Query Params: {params}")
                    print(f"Status Code: {response.status_code}")
                    print(f"Response: {response.text}")
                    print(f"{'=' * 80}\n")

                    logger.error(
                        "Unexpected ESB response status",
                        endpoint=endpoint,
                        status_code=response.status_code,
                        url=url,
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
                    # Print full URL for debugging
                    print(f"\n{'=' * 80}")
                    print("ESB REQUEST FAILED - TIMEOUT")
                    print(f"{'=' * 80}")
                    print(f"Method: {method}")
                    print(f"URL: {url}")
                    if params:
                        print(f"Query Params: {params}")
                    print(f"Error: Request timeout after {self.max_retries} attempts")
                    print(f"Timeout: {self.timeout}s")
                    print(f"{'=' * 80}\n")

                    logger.error(
                        "ESB request timeout, max retries exceeded",
                        endpoint=endpoint,
                        url=url,
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
                    # Print full URL for debugging
                    print(f"\n{'=' * 80}")
                    print("ESB REQUEST FAILED - REQUEST ERROR")
                    print(f"{'=' * 80}")
                    print(f"Method: {method}")
                    print(f"URL: {url}")
                    if params:
                        print(f"Query Params: {params}")
                    print(f"Error: {str(e)}")
                    print(f"Attempts: {attempt + 1}/{self.max_retries}")
                    print(f"{'=' * 80}\n")

                    logger.error(
                        "ESB request error, max retries exceeded",
                        endpoint=endpoint,
                        error=str(e),
                        url=url,
                    )
                    raise ESBClientError(
                        f"Request failed for {endpoint}: {str(e)}"
                    ) from e

        # Print full URL for debugging
        print(f"\n{'=' * 80}")
        print("ESB REQUEST FAILED - MAX RETRIES EXCEEDED")
        print(f"{'=' * 80}")
        print(f"Method: {method}")
        print(f"URL: {url}")
        if params:
            print(f"Query Params: {params}")
        print(f"Max Retries: {self.max_retries}")
        print(f"{'=' * 80}\n")

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

    async def get_integration(self, integration_type: str) -> list[dict[str, Any]]:
        """Fetch hotels from ESB getIntegration endpoint with credentials.

        This endpoint returns the list of hotels configured for a specific
        integration type along with their authentication credentials.

        Args:
            integration_type: Integration type identifier (e.g., "BITZ")

        Returns:
            List of hotel dictionaries with fields:
            - code: Hotel code
            - auth_id: Authentication ID (used as Ocp-Apim-Subscription-Key)
            - auth_username: Authentication username
            - hotel_id: Hotel ID
            - auth_password: Authentication password
            - integration_type: Integration type

        Raises:
            ESBClientError: If the API request fails
        """
        logger.info(
            "Fetching hotel list from getIntegration endpoint",
            integration_type=integration_type,
        )
        response = await self._make_request(
            "GET",
            "/pms-integration/1.0/getIntegration",
            params={"integration": integration_type},
        )

        # Extract hotel_list from result
        hotel_list = response.get("result", {}).get("hotel_list", [])

        logger.info(
            "Successfully fetched hotels from getIntegration",
            integration_type=integration_type,
            hotel_count=len(hotel_list),
        )
        return hotel_list

    async def get_hotel_parameters(self, hotel_code: str) -> dict[str, Any]:
        """Fetch hotel configuration from ESB getHotelConfig endpoint.

        This method calls the ESB API endpoint:
        GET /pms-integration/1.0/getHotelConfig?code={hotel_code}

        The response contains hotel_config array with key-value pairs.
        We extract KpisRecordDateMax to use as the lastImportDate.

        Args:
            hotel_code: The hotel code identifier

        Returns:
            Dictionary containing normalized import parameters:
            - lastImportDate: KpisRecordDateMax from config (or 2 years ago if not available)
            - minImportDate: None
            - maxImportDate: None
            - hotelCode: The hotel code
            - _raw_config: Original config dict for reference

        Raises:
            ESBClientError: If the API request fails
            ESBNotFoundError: If the hotel is not found
        """
        from datetime import datetime, timedelta

        logger.info("Fetching hotel config from ESB", hotel_code=hotel_code)

        # Call the ESB getHotelConfig endpoint
        response = await self._make_request(
            "GET",
            "/pms-integration/1.0/getHotelConfig",
            params={"code": hotel_code},
        )

        # Extract hotel_config array from response
        hotel_config_array = response.get("result", {}).get("hotel_config", [])

        # Convert array of {key, value} objects to dict
        config_dict = {item["key"]: item["value"] for item in hotel_config_array}

        # Get KpisRecordDateMax or calculate 2 years ago as fallback
        kpis_date = config_dict.get("KpisRecordDateMax")
        is_first_import = False  # Track if this is the first import

        if kpis_date:
            last_import_date = kpis_date
            logger.info(
                "Using KpisRecordDateMax as lastImportDate",
                hotel_code=hotel_code,
                kpis_record_date_max=kpis_date,
            )
        else:
            # Fallback: use 2 years ago
            two_years_ago = datetime.utcnow() - timedelta(days=730)
            last_import_date = two_years_ago.strftime("%Y-%m-%d")
            is_first_import = True  # No KpisRecordDateMax means this is the first import
            logger.warning(
                "KpisRecordDateMax not found, using 2 years ago as fallback (first import)",
                hotel_code=hotel_code,
                fallback_date=last_import_date,
                is_first_import=True,
            )

        # Return normalized format expected by the pipeline
        normalized_params = {
            "lastImportDate": last_import_date,
            "minImportDate": None,
            "maxImportDate": None,
            "hotelCode": hotel_code,
            "isFirstImport": is_first_import,  # Flag to indicate first import
            "_raw_config": config_dict,  # Keep original config for reference
        }

        logger.info(
            "Successfully fetched hotel parameters",
            hotel_code=hotel_code,
            last_import_date=last_import_date,
            config_keys=list(config_dict.keys())[:10],  # Log first 10 config keys
        )

        return normalized_params

    async def register_file(
        self,
        hotel_code: str,
        file_type: str,
        file_url: str,
        file_key: str,
        record_count: int,
        is_first_import: bool = False,
    ) -> dict[str, Any]:
        """Register an imported file with the ESB.

        Routes to the correct ESB endpoint based on file type:
        - segments -> /pms-integration/1.0/pmsSegment
        - reservations -> /pms-integration/1.0/pmsReservation
        - hotel-configs -> /pms-integration/1.0/pmsHotelConfig

        Args:
            hotel_code: The hotel code
            file_type: Type of file (segments, reservations, hotel-configs)
            file_url: S3 URL or path to the processed file
            file_key: S3 object key for the processed file
            record_count: Number of records in the file
            is_first_import: If True, sets complete=True (when KpisRecordDateMax was null/empty)

        Returns:
            Registration response from ESB

        Raises:
            ESBClientError: If the registration fails
        """
        from datetime import datetime

        # Map file types to ESB endpoints
        endpoint_map = {
            "segments": "/pms-integration/1.0/pmsSegment",
            "reservations": "/pms-integration/1.0/pmsReservation",
            "hotel-configs": "/pms-integration/1.0/pmsHotelConfig",
        }

        endpoint = endpoint_map.get(file_type)
        if not endpoint:
            logger.error(
                "Unknown file type for ESB registration",
                hotel_code=hotel_code,
                file_type=file_type,
                valid_types=list(endpoint_map.keys()),
            )
            raise ESBClientError(
                f"Unknown file type '{file_type}'. Valid types: {', '.join(endpoint_map.keys())}"
            )

        # Generate timestamp for record_date and last_updated
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        # Set complete flag: True if first import (KpisRecordDateMax was null/empty), False otherwise
        complete = is_first_import

        logger.info(
            "Registering file with ESB",
            hotel_code=hotel_code,
            file_type=file_type,
            endpoint=endpoint,
            record_count=record_count,
            is_first_import=is_first_import,
            complete=complete,
        )

        # Build payload based on ESB requirements - wrapped in "payload" key
        payload = {
            "payload": {
                "code": hotel_code,
                "record_date": ts,
                "last_updated": ts,
                "complete": complete,
                "file": file_key,
            }
        }

        response = await self._make_request("POST", endpoint, data=payload)

        logger.info(
            "Successfully registered file with ESB",
            hotel_code=hotel_code,
            file_type=file_type,
            endpoint=endpoint,
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

    async def clear_token_cache(self) -> None:
        """Clear cached OAuth token from Redis.

        This forces a fresh token to be fetched on the next ESB API request.
        Useful at process start to ensure we're not using stale cached tokens.
        """
        logger.info("Clearing ESB OAuth token cache")
        await self.token_manager.clear_cache()

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
