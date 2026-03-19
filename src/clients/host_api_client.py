"""Host PMS API client for data extraction."""

import time
from typing import Any, Optional

import httpx
from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class HostAPIClientError(Exception):
    """Base exception for Host API client errors."""

    pass


class HostAPIAuthenticationError(HostAPIClientError):
    """Raised when Host API authentication fails."""

    pass


class HostAPINotFoundError(HostAPIClientError):
    """Raised when Host API resource is not found."""

    pass


class HostAPIServerError(HostAPIClientError):
    """Raised when Host API returns a server error."""

    pass


class HostPMSAPIClient:
    """Client for Host PMS API endpoints."""

    def __init__(self, subscription_key: Optional[str] = None):
        """Initialize the Host PMS API client with settings.

        Args:
            subscription_key: Optional hotel-specific subscription key.
                            If not provided, uses default from settings.
        """
        # Prefer top-level (from .env HOST_API_*); fallback to nested host_pms.*
        base = (settings.host_api_base_url or settings.host_pms.base_url or "").strip()
        self.base_url = (base or "https://hostapi.azure-api.net/rms-v2").rstrip("/")

        # Use provided subscription_key, otherwise fallback to settings
        if subscription_key:
            self.subscription_key = subscription_key
        else:
            self.subscription_key = (
                (settings.host_api_subscription_key or settings.host_pms.subscription_key or "").strip()
                or "test-subscription-key-default"
            )

        self.timeout = settings.host_pms.request_timeout
        self.max_retries = settings.host_pms.max_retries
        self.retry_backoff_base = 2  # Exponential backoff base

    def _get_headers(self) -> dict[str, str]:
        """Get default headers for Host PMS API requests.

        Returns:
            Dictionary of HTTP headers including subscription key.
        """
        return {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": self.subscription_key,
            "User-Agent": "HostPMSConnector/1.0",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        hotel_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Make an HTTP request to the Host PMS API with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint path (without base URL)
            data: Request body data (for POST requests)
            params: Query parameters

        Returns:
            JSON response as a dictionary

        Raises:
            HostAPIAuthenticationError: If authentication fails
            HostAPINotFoundError: If resource not found
            HostAPIServerError: If server error occurs
            HostAPIClientError: For other API errors
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                        params=params,
                    )

                    # Handle authentication errors
                    if response.status_code == 401:
                        logger.error(
                            "Host API authentication failed",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPIAuthenticationError(
                            f"Authentication failed for {endpoint}: Invalid subscription key"
                        )

                    # Handle forbidden errors
                    if response.status_code == 403:
                        logger.error(
                            "Host API forbidden",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPIAuthenticationError(
                            f"Access forbidden for {endpoint}: Check subscription key permissions"
                        )

                    # Handle not found errors
                    if response.status_code == 404:
                        logger.warning(
                            "Host API resource not found",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPINotFoundError(
                            f"Resource not found: {endpoint}"
                        )

                    # Handle server errors with retry
                    if response.status_code >= 500:
                        if attempt < self.max_retries - 1:
                            wait_time = self.retry_backoff_base ** attempt
                            logger.warning(
                                "Host API server error, retrying",
                                hotel_code=hotel_code,
                                endpoint=endpoint,
                                status_code=response.status_code,
                                attempt=attempt + 1,
                                max_retries=self.max_retries,
                                wait_seconds=wait_time,
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                "Host API server error, max retries exceeded",
                                hotel_code=hotel_code,
                                endpoint=endpoint,
                                status_code=response.status_code,
                            )
                            raise HostAPIServerError(
                                f"Server error at {endpoint}: {response.text}"
                            )

                    # Handle client errors (non-auth, non-404)
                    if 400 <= response.status_code < 500:
                        logger.error(
                            "Host API client error",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                            response_text=response.text[:200],  # Limit error text
                        )
                        raise HostAPIClientError(
                            f"Client error at {endpoint}: {response.text}"
                        )

                    # Handle success
                    if response.status_code in (200, 201, 204):
                        logger.debug(
                            "Host API request successful",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            method=method,
                            status_code=response.status_code,
                        )
                        if response.text:
                            return response.json()
                        return {}

                    # Unexpected status code
                    logger.error(
                        "Unexpected Host API response status",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        status_code=response.status_code,
                    )
                    raise HostAPIClientError(
                        f"Unexpected response from {endpoint}: {response.status_code}"
                    )

            except httpx.TimeoutException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "Host API request timeout, retrying",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        wait_seconds=wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        "Host API request timeout, max retries exceeded",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                    )
                    raise HostAPIClientError(
                        f"Request timeout for {endpoint}"
                    ) from e

            except httpx.RequestError as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "Host API request error, retrying",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        error=str(e),
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        wait_seconds=wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        "Host API request error, max retries exceeded",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        error=str(e),
                    )
                    raise HostAPIClientError(
                        f"Request failed for {endpoint}: {str(e)}"
                    ) from e

        raise HostAPIClientError(f"Failed to complete request to {endpoint}")

    async def _make_request_async(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        hotel_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Make an async HTTP request to the Host PMS API with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, etc.)
            endpoint: API endpoint path (without base URL)
            data: Request body data (for POST requests)
            params: Query parameters
            hotel_code: Hotel code for logging context

        Returns:
            JSON response as a dictionary

        Raises:
            HostAPIAuthenticationError: If authentication fails
            HostAPINotFoundError: If resource not found
            HostAPIServerError: If server error occurs
            HostAPIClientError: For other API errors
        """
        import asyncio

        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

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
                            "Host API authentication failed",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPIAuthenticationError(
                            f"Authentication failed for {endpoint}: Invalid subscription key"
                        )

                    # Handle forbidden errors
                    if response.status_code == 403:
                        logger.error(
                            "Host API forbidden",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPIAuthenticationError(
                            f"Access forbidden for {endpoint}: Check subscription key permissions"
                        )

                    # Handle not found errors
                    if response.status_code == 404:
                        logger.warning(
                            "Host API resource not found",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                        )
                        raise HostAPINotFoundError(
                            f"Resource not found: {endpoint}"
                        )

                    # Handle server errors with retry
                    if response.status_code >= 500:
                        if attempt < self.max_retries - 1:
                            wait_time = self.retry_backoff_base ** attempt
                            logger.warning(
                                "Host API server error, retrying",
                                hotel_code=hotel_code,
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
                                "Host API server error, max retries exceeded",
                                hotel_code=hotel_code,
                                endpoint=endpoint,
                                status_code=response.status_code,
                            )
                            raise HostAPIServerError(
                                f"Server error at {endpoint}: {response.text}"
                            )

                    # Handle client errors (non-auth, non-404)
                    if 400 <= response.status_code < 500:
                        logger.error(
                            "Host API client error",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            status_code=response.status_code,
                            response_text=response.text[:200],  # Limit error text
                        )
                        raise HostAPIClientError(
                            f"Client error at {endpoint}: {response.text}"
                        )

                    # Handle success
                    if response.status_code in (200, 201, 204):
                        logger.debug(
                            "Host API request successful",
                            hotel_code=hotel_code,
                            endpoint=endpoint,
                            method=method,
                            status_code=response.status_code,
                        )
                        if response.text:
                            return response.json()
                        return {}

                    # Unexpected status code
                    logger.error(
                        "Unexpected Host API response status",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        status_code=response.status_code,
                    )
                    raise HostAPIClientError(
                        f"Unexpected response from {endpoint}: {response.status_code}"
                    )

            except httpx.TimeoutException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "Host API request timeout, retrying",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        wait_seconds=wait_time,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        "Host API request timeout, max retries exceeded",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                    )
                    raise HostAPIClientError(
                        f"Request timeout for {endpoint}"
                    ) from e

            except httpx.RequestError as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_base ** attempt
                    logger.warning(
                        "Host API request error, retrying",
                        hotel_code=hotel_code,
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
                        "Host API request error, max retries exceeded",
                        hotel_code=hotel_code,
                        endpoint=endpoint,
                        error=str(e),
                    )
                    raise HostAPIClientError(
                        f"Request failed for {endpoint}: {str(e)}"
                    ) from e

        raise HostAPIClientError(f"Failed to complete request to {endpoint}")

    def get_hotel_config(self, hotel_code: str) -> dict[str, Any]:
        """Fetch hotel configuration from Host PMS API.

        Includes information about:
        - Hotel structure (rooms, room types, categories, etc.)
        - Segments (agencies, channels, companies, packages, etc.)
        - Rate plans and pricing configurations

        Args:
            hotel_code: The hotel code identifier

        Returns:
            Hotel configuration dictionary from the API response

        Raises:
            HostAPIClientError: If the API request fails
        """
        logger.info("Fetching hotel config from Host PMS API", hotel_code=hotel_code)
        params = {"hotelCode": hotel_code}
        response = self._make_request(
            "GET", "/ExternalRms/Config", params=params, hotel_code=hotel_code
        )
        logger.info(
            "Successfully fetched hotel config",
            hotel_code=hotel_code,
        )
        return response

    def get_reservations(
        self,
        hotel_code: str,
        update_from: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch all reservations from Host PMS API with pagination support.

        The API returns reservations in pages of ~100 per request. This method
        sequentially fetches all pages and combines them into a single response.

        Args:
            hotel_code: The hotel code identifier
            update_from: ISO format date/time string for incremental sync (e.g., "2024-01-01T00:00:00Z")
                        Use "1900-01-01T00:00:00Z" for first-time import
                        If None, fetches all reservations

        Returns:
            Combined reservations data from all pages

        Raises:
            HostAPIClientError: If the API request fails
        """
        logger.info(
            "Fetching all reservations from Host PMS API with pagination",
            hotel_code=hotel_code,
            update_from=update_from,
        )

        page_size = 100  # API returns ~100 rows per request

        # Fetch first page to determine total rows
        params = {
            "start": 0,
            "limit": page_size,
        }
        if update_from:
            params["updateFrom"] = update_from

        logger.debug(
            "Fetching first reservation page",
            hotel_code=hotel_code,
            page_number=1,
        )

        first_response = self._make_request(
            "GET", "/ExternalRms/Reservation", params=params, hotel_code=hotel_code
        )

        first_page_reservations = first_response.get("Reservations", [])
        if not first_page_reservations:
            logger.info(
                "No reservations found",
                hotel_code=hotel_code,
            )
            return {"Reservations": []}

        # Get total rows from first reservation
        total_rows = first_page_reservations[0].get("TotalRows") if first_page_reservations else None

        if total_rows is None:
            # If TotalRows is missing, use the count of first page as total
            total_rows = len(first_page_reservations)
            logger.warning(
                "TotalRows field missing from API response, using first page count",
                hotel_code=hotel_code,
                first_page_count=total_rows,
            )

        total_pages = (total_rows // page_size) + (1 if total_rows % page_size else 0) if total_rows else 1

        logger.info(
            "Pagination started",
            hotel_code=hotel_code,
            first_page_count=len(first_page_reservations),
            total_rows=total_rows,
            page_size=page_size,
            total_pages=total_pages,
        )

        # If only one page, return immediately
        if total_pages == 1:
            logger.info(
                "Successfully fetched all reservations",
                hotel_code=hotel_code,
                total_reservations=len(first_page_reservations),
                total_rows=total_rows,
                pages_fetched=1,
                update_from=update_from,
            )
            return {"Reservations": first_page_reservations}

        # Fetch remaining pages sequentially
        all_reservations = first_page_reservations.copy()
        failed_pages = []

        logger.debug(
            "Starting sequential fetch for remaining pages",
            hotel_code=hotel_code,
            total_pages_remaining=total_pages - 1,
        )

        for page_number in range(2, total_pages + 1):
            try:
                start_index = (page_number - 1) * page_size

                logger.debug(
                    "Fetching reservation page",
                    hotel_code=hotel_code,
                    page_number=page_number,
                    start=start_index,
                    limit=page_size,
                )

                params = {
                    "start": start_index,
                    "limit": page_size,
                }
                if update_from:
                    params["updateFrom"] = update_from

                response = self._make_request(
                    "GET", "/ExternalRms/Reservation", params=params, hotel_code=hotel_code
                )

                reservations = response.get("Reservations", [])

                all_reservations.extend(reservations)

            except Exception as e:
                logger.warning(
                    "Failed to fetch page",
                    hotel_code=hotel_code,
                    page_number=page_number,
                    error=str(e),
                )
                failed_pages.append(e)

        logger.info(
            "Successfully fetched all reservations",
            hotel_code=hotel_code,
            total_reservations=len(all_reservations),
            total_rows=total_rows,
            pages_fetched=total_pages,
            failed_pages=len(failed_pages),
            update_from=update_from,
        )

        # Return response in same format as single-page response
        return {"Reservations": all_reservations}

    def get_inventory(
        self,
        from_date: str,
        to_date: str,
        rate_code: Optional[str] = None,
        hotel_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch room inventory and availability from Host PMS API.

        Includes:
        - Room availability per day
        - Pricing per day
        - Out-of-inventory (OOI) and out-of-order (OOO) information

        The API does not require hotelCode — it is used for logging only.
        Callers should split large date ranges into 30-day windows and call this
        method once per window per rate code (ConfigType=RATECODE from get_hotel_config).

        Args:
            from_date: Start date in ISO format (e.g., "2024-01-01")
            to_date: End date in ISO format (e.g., "2024-01-31")
            rate_code: Optional single rate code (ConfigType=RATECODE) to filter inventory by.
            hotel_code: Optional hotel code used for logging context only (not sent as query param).

        Returns:
            Inventory data from the API response

        Raises:
            HostAPIClientError: If the API request fails
        """
        params: dict[str, Any] = {
            "fromDate": from_date,
            "toDate": to_date,
        }
        if rate_code:
            params["rateCode"] = rate_code

        # Build and print the full request URL for debugging
        full_url = str(httpx.URL(f"{self.base_url}/ExternalRms/InventoryGrid", params=params))
        print(f"[get_inventory] GET {full_url}")

        logger.info(
            "Fetching inventory from Host PMS API",
            hotel_code=hotel_code,
            from_date=from_date,
            to_date=to_date,
            rate_code=rate_code,
            url=full_url,
        )

        response = self._make_request(
            "GET", "/ExternalRms/InventoryGrid", params=params, hotel_code=hotel_code
        )
        logger.info(
            "Successfully fetched inventory",
            hotel_code=hotel_code,
            from_date=from_date,
            to_date=to_date,
            rate_code=rate_code,
        )
        return response

    async def get_inventory_all_rates(
        self,
        config_response: Any,
        from_date: str,
        to_date: str,
    ) -> dict[str, Any]:
        """Fetch inventory for all rate codes in parallel with concurrency limit.

        Extracts rate codes from config, then fetches inventory for each rate code
        in parallel with a maximum of 5 concurrent requests.

        Automatically splits date ranges larger than 30 days into 30-day chunks
        to comply with the InventoryGrid API's "Max days: 30" limitation.

        Args:
            config_response: Hotel configuration response (HotelConfigResponse or dict)
            from_date: Start date in ISO format (e.g., "2024-01-01")
            to_date: End date in ISO format (e.g., "2024-01-31")

        Returns:
            Combined inventory data with all room inventories

        Raises:
            HostAPIClientError: If the API requests fail
        """
        import asyncio
        from datetime import datetime, timedelta
        from src.models.host.config import HotelConfigResponse

        # Convert dict to HotelConfigResponse if needed
        if isinstance(config_response, dict):
            config_model = HotelConfigResponse(**config_response)
        else:
            config_model = config_response

        # Extract hotel code
        hotel_code = config_model.hotel_info.hotel_code

        # Extract rate codes from config (ConfigType=RATECODE)
        rate_codes = [item.code for item in config_model.get_config_by_type("RATECODE")]

        if not rate_codes:
            logger.warning(
                "No rate codes found in config, fetching inventory without rate filter",
                hotel_code=hotel_code,
            )
            # Fallback: fetch without rate code filter
            return self.get_inventory(
                hotel_code=hotel_code,
                start_date=from_date,
                end_date=to_date,
            )

        # Split date range into 30-day chunks
        start_dt = datetime.fromisoformat(from_date.replace('Z', '+00:00') if 'Z' in from_date else from_date)
        end_dt = datetime.fromisoformat(to_date.replace('Z', '+00:00') if 'Z' in to_date else to_date)

        date_chunks = []
        current_start = start_dt
        while current_start < end_dt:
            current_end = min(current_start + timedelta(days=30), end_dt)
            date_chunks.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))
            current_start = current_end + timedelta(days=1)

        logger.info(
            "Fetching inventory for all rate codes (ConfigType=RATECODE) in parallel",
            hotel_code=hotel_code,
            rate_count=len(rate_codes),
            rate_codes=rate_codes[:10] if len(rate_codes) > 10 else rate_codes,  # Log first 10 rate codes
            from_date=from_date,
            to_date=to_date,
            date_chunks=len(date_chunks),
            max_concurrent=5,
        )

        # Create semaphore to limit concurrent requests to 5
        semaphore = asyncio.Semaphore(5)

        async def fetch_inventory_for_rate_and_chunk(rate_code: str, chunk_from: str, chunk_to: str) -> dict[str, Any]:
            """Fetch inventory for a single rate code and date chunk with semaphore."""
            async with semaphore:
                try:
                    logger.debug(
                        "Fetching inventory for rate code and date chunk",
                        hotel_code=hotel_code,
                        rate_code=rate_code,
                        chunk_from=chunk_from,
                        chunk_to=chunk_to,
                    )

                    params: dict[str, Any] = {
                        "fromDate": chunk_from,
                        "toDate": chunk_to,
                    }
                    if rate_code:
                        params["rateCode"] = rate_code

                    response = await self._make_request_async(
                        "GET", "/ExternalRms/InventoryGrid", params=params, hotel_code=hotel_code
                    )

                    # Handle both list and dict responses
                    if isinstance(response, list):
                        # API returned a list directly
                        item_count = len(response)
                        normalized_response = {"roomInventories": response}
                    elif isinstance(response, dict):
                        # API returned a dict with roomInventories key
                        item_count = len(response.get("roomInventories", []))
                        normalized_response = response
                    else:
                        # Unexpected response type
                        logger.warning(
                            "Unexpected response type from InventoryGrid API",
                            hotel_code=hotel_code,
                            rate_code=rate_code,
                            response_type=type(response).__name__,
                        )
                        normalized_response = {"roomInventories": []}
                        item_count = 0

                    logger.debug(
                        "Successfully fetched inventory for rate code and chunk",
                        hotel_code=hotel_code,
                        rate_code=rate_code,
                        chunk_from=chunk_from,
                        chunk_to=chunk_to,
                        item_count=item_count,
                    )

                    return normalized_response

                except Exception as e:
                    logger.warning(
                        "Failed to fetch inventory for rate code and chunk",
                        hotel_code=hotel_code,
                        rate_code=rate_code,
                        chunk_from=chunk_from,
                        chunk_to=chunk_to,
                        error=str(e),
                    )
                    # Return empty response for failed requests
                    return {"roomInventories": []}

        # Create tasks for all rate codes × all date chunks
        tasks = []
        # for rate_code in rate_codes:
            # for chunk_from, chunk_to in date_chunks:
                # tasks.append(fetch_inventory_for_rate_and_chunk(rate_code, chunk_from, chunk_to))

        logger.info(
            "Fetching inventory data",
            hotel_code=hotel_code,
            total_requests=len(tasks),
            rate_codes=len(rate_codes),
            date_chunks=len(date_chunks),
        )

        # Fetch inventory for all rate codes × date chunks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Combine all room inventories from all requests
        all_room_inventories = []
        for result in results:
            if result and "roomInventories" in result:
                all_room_inventories.extend(result["roomInventories"])

        logger.info(
            "Successfully fetched inventory for all rate codes and date chunks",
            hotel_code=hotel_code,
            rate_count=len(rate_codes),
            date_chunks=len(date_chunks),
            total_requests=len(tasks),
            total_items=len(all_room_inventories),
        )

        return {"roomInventories": all_room_inventories}

    def get_revenue(
        self,
        hotel_code: str,
        update_from: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch financial transactions from Host PMS API.

        Only includes room-related transactions (SalesGroup = 0).

        Args:
            hotel_code: The hotel code identifier
            update_from: ISO format date/time string for incremental sync (e.g., "2024-01-01T00:00:00Z")

        Returns:
            Revenue/transaction data from the API response

        Raises:
            HostAPIClientError: If the API request fails
        """
        logger.info(
            "Fetching revenue from Host PMS API",
            hotel_code=hotel_code,
            update_from=update_from,
        )
        params = {
            "hotelCode": hotel_code,
            "salesGroup": 0,  # Room revenue only
        }
        if update_from:
            params["updateFrom"] = update_from

        response = self._make_request(
            "GET", "/ExternalRms/Revenue", params=params, hotel_code=hotel_code
        )

        # Extract revenue list from response
        revenue = response.get("revenue", [])
        logger.info(
            "Successfully fetched revenue",
            hotel_code=hotel_code,
            revenue_count=len(revenue),
            update_from=update_from,
        )
        return response

    def get_stat_daily(
        self,
        hotel_date_filter: str,
        hotel_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch daily statistics from Host PMS API (synchronous).

        Returns statistical data for a specific hotel date, including occupancy
        and revenue information. Multiple records may be returned for the same
        reservation with different RecordTypes and ChargeCodes.

        Args:
            hotel_date_filter: Date to fetch statistics for (ISO format: "YYYY-MM-DD")
            hotel_code: Optional hotel code for logging context

        Returns:
            List of StatDaily records from the API response

        Raises:
            HostAPIClientError: If the API request fails

        Note:
            This is the synchronous version. Use get_stat_daily_async() for async contexts.
        """
        logger.info(
            "Fetching StatDaily from Host PMS API",
            hotel_code=hotel_code,
            hotel_date_filter=hotel_date_filter,
        )
        params = {
            "hoteldatefilter": hotel_date_filter,
        }

        response = self._make_request(
            "GET", "/ExternalRms/StatDaily", params=params, hotel_code=hotel_code
        )

        # Response is a list of records
        if isinstance(response, list):
            record_count = len(response)
        else:
            record_count = 0

        logger.info(
            "Successfully fetched StatDaily",
            hotel_code=hotel_code,
            hotel_date_filter=hotel_date_filter,
            record_count=record_count,
        )
        return response

    async def get_stat_daily_async(
        self,
        hotel_date_filter: str,
        hotel_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """Fetch daily statistics from Host PMS API (asynchronous).

        Returns statistical data for a specific hotel date, including occupancy
        and revenue information. Multiple records may be returned for the same
        reservation with different RecordTypes and ChargeCodes.

        This is the async version that doesn't block the event loop, enabling
        parallel processing of multiple hotels and dates.

        Args:
            hotel_date_filter: Date to fetch statistics for (ISO format: "YYYY-MM-DD")
            hotel_code: Optional hotel code for logging context

        Returns:
            List of StatDaily records from the API response

        Raises:
            HostAPIClientError: If the API request fails
        """
        logger.info(
            "Fetching StatDaily from Host PMS API (async)",
            hotel_code=hotel_code,
            hotel_date_filter=hotel_date_filter,
        )
        params = {
            "hoteldatefilter": hotel_date_filter,
        }

        response = await self._make_request_async(
            "GET", "/ExternalRms/StatDaily", params=params, hotel_code=hotel_code
        )

        # Response is a list of records
        if isinstance(response, list):
            record_count = len(response)
        else:
            record_count = 0

        logger.info(
            "Successfully fetched StatDaily (async)",
            hotel_code=hotel_code,
            hotel_date_filter=hotel_date_filter,
            record_count=record_count,
        )
        return response

    def get_stat_summary(
        self,
        from_date: str,
        to_date: str,
        hotel_code: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch daily statistics summary from Host PMS API.

        Returns aggregated statistics per day with room nights and revenue totals.
        Used for validation of StatDaily transformer results.

        Args:
            from_date: Start date (ISO format: "YYYY-MM-DD")
            to_date: End date (ISO format: "YYYY-MM-DD")
            hotel_code: Optional hotel code for logging context

        Returns:
            List of StatSummary records from the API response

        Raises:
            HostAPIClientError: If the API request fails
        """
        logger.info(
            "Fetching StatSummary from Host PMS API",
            hotel_code=hotel_code,
            from_date=from_date,
            to_date=to_date,
        )
        params = {
            "fromdate": from_date,
            "todate": to_date,
        }

        response = self._make_request(
            "GET", "/ExternalRms/StatSummary", params=params, hotel_code=hotel_code
        )

        # Response is a list of records
        if isinstance(response, list):
            record_count = len(response)
        else:
            record_count = 0

        logger.info(
            "Successfully fetched StatSummary",
            hotel_code=hotel_code,
            from_date=from_date,
            to_date=to_date,
            record_count=record_count,
        )
        return response
