"""Main orchestrator service for the Host PMS connector ETL pipeline."""

import asyncio
import os
from datetime import datetime
from typing import Any, Optional

from structlog import get_logger

from src.aws import S3Manager, SQSManager
from src.config import settings
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import Pipeline, PipelineContext
from src.services.pipeline.steps import (
    FetchParametersStep,
    ProcessConfigStep,
    ProcessSegmentsStep,
    ProcessStatDailyStep,
    SendNotificationsStep
)

logger = get_logger(__name__)


class OrchestrationError(Exception):
    """Raised when orchestration fails."""

    pass


class HostPMSConnectorOrchestrator:
    """Main orchestrator for the Host PMS to Climber ETL pipeline.

    This orchestrator uses a pipeline pattern to process hotels through
    multiple discrete steps, making the code more maintainable and testable.
    """

    SUMMARY_DIR = "logs"

    def __init__(self):
        """Initialize the orchestrator with all required services."""
        self.esb_client = ClimberESBClient()
        self.s3_manager = S3Manager()
        self.sqs_manager = SQSManager()
        self._summary_lock = asyncio.Lock()
        self._summary_file: str | None = None

    def _init_summary_file(self, total_hotels: int) -> None:
        """Create the summary file with a header at the start of execution."""
        os.makedirs(self.SUMMARY_DIR, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self._summary_file = os.path.join(self.SUMMARY_DIR, f"execution_summary_{ts}.txt")
        started = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(self._summary_file, "w") as f:
            f.write(f"{'='*80}\n")
            f.write(f"  Host PMS Connector — Execution Summary\n")
            f.write(f"  Started: {started}\n")
            f.write(f"  Total hotels: {total_hotels}\n")
            f.write(f"{'='*80}\n\n")

    async def _append_hotel_summary(self, result: dict[str, Any]) -> None:
        """Append a hotel's execution summary to the file (thread-safe)."""
        hotel = result.get("hotel_code", "UNKNOWN")
        success = result.get("success", False)
        duration = result.get("duration_seconds", "N/A")
        status = "OK" if success else "FAILED"
        errors = result.get("errors", [])
        stats = result.get("stats", {})

        lines = []
        lines.append(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {hotel:20s} {status}")
        if duration != "N/A":
            lines[-1] += f"  ({duration:.1f}s)"

        # Stats summary
        stat_daily = stats.get("stat_daily", {})
        if stat_daily:
            raw = stat_daily.get("raw_record_count", 0)
            res = stat_daily.get("reservations_created", 0)
            lines.append(f"         records={raw}  reservations={res}")

        # Errors
        for err in errors:
            step = err.get("step", "?")
            msg = err.get("message", "")
            err_type = err.get("error_type", "")
            label = f"{err_type} " if err_type else ""
            lines.append(f"         ERROR [{step}] {label}{msg[:120]}")

        lines.append("")

        block = "\n".join(lines) + "\n"

        async with self._summary_lock:
            with open(self._summary_file, "a") as f:
                f.write(block)

    def _build_pipeline(self, host_api_client: HostPMSAPIClient) -> Pipeline:
        """Build the ETL pipeline with all processing steps.

        Args:
            host_api_client: Host PMS API client with hotel-specific credentials

        Returns:
            Configured pipeline ready to execute
        """
        steps = [
            # Step 1: Fetch import parameters (required)
            # Fetches lastImportDate, minImportDate, maxImportDate from ESB
            # Calculates date ranges for all subsequent steps
            FetchParametersStep(self.esb_client),
            # Step 2: Process hotel config (optional)
            # Extracts segments and room inventory from /Config endpoint
            # Inventory is extracted from CATEGORY items and uploaded to hotel-configs
            ProcessConfigStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 3: Process inventory grid from API (DEPRECATED)
            # ProcessInventoryGridStep is deprecated - inventory now comes from Config step
            # ProcessInventoryGridStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 4: Process segments (optional)
            ProcessSegmentsStep(self.esb_client, self.s3_manager),
            # Step 5: Process StatDaily and convert to reservations (optional)
            # This step replaces the old ProcessReservationsStep
            # StatDaily is the primary source for reservation data
            # Uses calculated date ranges from FetchParametersStep
            ProcessStatDailyStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 6: Update last import date (optional)
            # UpdateImportDateStep(self.esb_client),
            # Step 7: Send SQS notifications (optional)
            SendNotificationsStep(self.sqs_manager),
        ]

        return Pipeline(name="HostPMSConnectorPipeline", steps=steps)

    async def process_hotel(
        self,
        hotel_code: str,
        host_api_client: Optional[HostPMSAPIClient] = None,
        worker_id: Optional[int] = None,
    ) -> dict[str, Any]:
        """Process a single hotel through the ETL pipeline.

        This method uses a pipeline pattern to execute discrete processing steps.
        Each step is independent, testable, and can fail gracefully without
        affecting other steps.

        Args:
            hotel_code: Hotel code to process
            host_api_client: Optional Host PMS API client with hotel-specific credentials.
                           If not provided, credentials will be fetched from getIntegration.

        Returns:
            Dictionary with processing results and statistics

        Raises:
            OrchestrationError: If critical processing fails
        """
        logger.info(
            "Starting hotel processing",
            hotel_code=hotel_code,
            worker_id=worker_id,
        )

        try:
            # If no client provided, fetch credentials from getIntegration
            if host_api_client is None:
                logger.info(
                    "No API client provided, fetching credentials from getIntegration",
                    hotel_code=hotel_code,
                )
                hotels = await self.esb_client.get_integration("BITZ")
                hotel_data = next((h for h in hotels if h.get("code") == hotel_code), None)

                if not hotel_data:
                    raise OrchestrationError(
                        f"Hotel {hotel_code} not found in integration endpoint"
                    )

                subscription_key = hotel_data.get("auth_id")
                if not subscription_key:
                    raise OrchestrationError(
                        f"No auth_id found for hotel {hotel_code}"
                    )

                logger.info(
                    "Creating Host API client with hotel-specific credentials",
                    hotel_code=hotel_code,
                )
                host_api_client = HostPMSAPIClient(subscription_key=subscription_key)

            # Validate subscription key before running pipeline
            logger.info(
                "Validating subscription key with Host PMS API",
                hotel_code=hotel_code,
            )
            try:
                # Test credentials by fetching hotel config (lightweight call)
                await asyncio.to_thread(host_api_client.get_hotel_config, hotel_code)
                logger.info(
                    "Subscription key validated successfully",
                    hotel_code=hotel_code,
                )
            except Exception as e:
                from src.clients.host_api_client import HostAPIAuthenticationError

                # Check if it's an authentication error
                if isinstance(e, HostAPIAuthenticationError):
                    logger.error(
                        "Invalid subscription key - skipping hotel",
                        hotel_code=hotel_code,
                        error=str(e),
                    )
                    return {
                        "hotel_code": hotel_code,
                        "success": False,
                        "errors": [{
                            "step": "authentication",
                            "message": f"Invalid subscription key for hotel {hotel_code}. Authentication failed with Host PMS API. Hotel skipped. Please verify credentials in ESB getIntegration endpoint.",
                            "error_type": "AUTHENTICATION_FAILED"
                        }],
                        "stats": {},
                        "s3_uploads": {},
                        "sqs_messages": [],
                    }
                else:
                    # Re-raise non-auth errors (network issues, etc.)
                    raise

            # Create pipeline context
            context = PipelineContext(hotel_code=hotel_code)
            context.worker_id = worker_id

            # Build and execute pipeline
            pipeline = self._build_pipeline(host_api_client)
            context = await pipeline.execute(context)

            # Get results from context
            results = context.get_results()

            logger.info(
                "Hotel processing complete",
                hotel_code=hotel_code,
                success=results["success"],
                error_count=len(results["errors"]),
            )

            return results

        except Exception as e:
            logger.error(
                "Unexpected error processing hotel",
                hotel_code=hotel_code,
                error=str(e),
                exc_info=True,
            )
            return {
                "hotel_code": hotel_code,
                "success": False,
                "errors": [{"step": "orchestrator", "message": f"Unexpected error: {str(e)}"}],
                "stats": {},
                "s3_uploads": {},
                "sqs_messages": [],
            }

    async def process_single_hotel(
        self,
        hotel_code: str,
        integration_type: str = "BITZ",
    ) -> dict[str, Any]:
        """Process a single hotel manually through the ETL pipeline.

        This is a convenience method for manual single-hotel processing.
        It fetches the hotel's credentials from getIntegration and processes it.

        Args:
            hotel_code: Hotel code to process
            integration_type: Integration type to use (default: "BITZ")

        Returns:
            Dictionary with processing results and statistics

        Raises:
            OrchestrationError: If critical processing fails
        """
        logger.info(
            "Starting manual single-hotel processing",
            hotel_code=hotel_code,
            integration_type=integration_type,
        )

        # Clear Redis token cache at process start to ensure fresh tokens
        await self.esb_client.clear_token_cache()

        # process_hotel() will fetch credentials from getIntegration if no client provided
        return await self.process_hotel(hotel_code)

    async def _process_hotel_with_worker(
        self,
        worker_pool: asyncio.Queue,
        hotel_code: str,
        subscription_key: str,
    ) -> dict[str, Any]:
        """Process a single hotel with a worker ID from the pool.

        The worker pool acts as both concurrency limiter and ID assignment.
        Each hotel gets a unique worker_id (1..N) that appears in logs.

        Args:
            worker_pool: Queue of available worker IDs
            hotel_code: Hotel code to process
            subscription_key: Hotel-specific subscription key (auth_id)

        Returns:
            Dictionary with processing results
        """
        worker_id = await worker_pool.get()
        try:
            logger.info(
                "Creating Host API client with hotel-specific credentials",
                hotel_code=hotel_code,
                worker_id=worker_id,
            )
            host_api_client = HostPMSAPIClient(subscription_key=subscription_key)

            result = await self.process_hotel(hotel_code, host_api_client, worker_id=worker_id)
            await self._append_hotel_summary(result)
            return result

        except Exception as e:
            logger.error(
                "Failed to process hotel",
                hotel_code=hotel_code,
                worker_id=worker_id,
                error=str(e),
            )
            result = {
                "hotel_code": hotel_code,
                "success": False,
                "errors": [{"step": "orchestrator", "message": str(e)}],
            }
            await self._append_hotel_summary(result)
            return result
        finally:
            worker_pool.put_nowait(worker_id)

    async def process_all_hotels(self, integration_type: str = "BITZ") -> dict[str, Any]:
        """Process all configured hotels through the ETL pipeline in parallel.

        Fetches hotels from the getIntegration endpoint and processes each
        with hotel-specific credentials in parallel (max 3 concurrent hotels).
        Uses auth_id as the Ocp-Apim-Subscription-Key.

        Args:
            integration_type: Integration type to fetch (default: "BITZ")

        Returns:
            Dictionary with aggregated results for all hotels
        """
        logger.info(
            "Starting batch processing of all hotels",
            integration_type=integration_type,
        )

        # Clear Redis token cache at process start to ensure fresh tokens
        await self.esb_client.clear_token_cache()

        all_results = {
            "total_hotels": 0,
            "successful_hotels": 0,
            "failed_hotels": 0,
            "authentication_failures": 0,
            "hotels": [],
            "start_time": datetime.utcnow().isoformat(),
        }

        try:
            # Step 1: Get list of configured hotels from ESB getIntegration endpoint
            logger.info(
                "Fetching hotel list from getIntegration endpoint",
                integration_type=integration_type,
            )
            hotels = await self.esb_client.get_integration(integration_type)
            all_results["total_hotels"] = len(hotels)

            logger.info("Successfully fetched hotels", hotel_count=len(hotels))

            # Initialize execution summary file
            self._init_summary_file(len(hotels))

            # TEMP DEBUG: Print hotel credentials
            print("\n" + "="*80)
            print("DEBUG: getIntegration Response - Hotel Credentials")
            print("="*80)
            for hotel in hotels:
                code = hotel.get("code", "N/A")
                auth_id = hotel.get("auth_id", "N/A")
                print(f"  Hotel Code: {code:15} | auth_id: {auth_id}")
            print("="*80 + "\n")

            # Step 2: Process hotels in parallel with worker ID pool
            max_concurrent = settings.host_pms.max_concurrent_hotels
            worker_pool = asyncio.Queue()
            for i in range(1, max_concurrent + 1):
                worker_pool.put_nowait(i)

            # Build list of tasks for valid hotels
            tasks = []
            for hotel in hotels:
                hotel_code = hotel.get("code")
                subscription_key = hotel.get("auth_id")

                if not hotel_code:
                    logger.warning("Skipping hotel with no code", hotel=hotel)
                    continue

                if not subscription_key:
                    logger.warning(
                        "Skipping hotel - no auth_id found, continuing with next hotel",
                        hotel_code=hotel_code,
                    )
                    all_results["hotels"].append(
                        {
                            "hotel_code": hotel_code,
                            "success": False,
                            "errors": [{"step": "orchestrator", "message": "No auth_id found - skipped"}],
                        }
                    )
                    all_results["failed_hotels"] += 1
                    continue

                # Create task for this hotel
                task = self._process_hotel_with_worker(
                    worker_pool, hotel_code, subscription_key
                )
                tasks.append(task)

            # Process all hotels in parallel
            logger.info(
                "Starting parallel hotel processing",
                total_tasks=len(tasks),
                max_concurrent=max_concurrent,
            )

            results = await asyncio.gather(*tasks, return_exceptions=False)

            # Aggregate results
            for result in results:
                all_results["hotels"].append(result)

                if result["success"]:
                    all_results["successful_hotels"] += 1
                else:
                    all_results["failed_hotels"] += 1

                    # Track authentication failures separately
                    if result.get("errors"):
                        for error in result["errors"]:
                            if error.get("error_type") == "AUTHENTICATION_FAILED":
                                all_results["authentication_failures"] += 1
                                logger.warning(
                                    "Authentication failure - hotel skipped",
                                    hotel_code=result.get("hotel_code"),
                                    message=error.get("message"),
                                )
                                break

            all_results["end_time"] = datetime.utcnow().isoformat()

            # Write final totals to summary file
            with open(self._summary_file, "a") as f:
                f.write(f"{'='*80}\n")
                f.write(f"  Finished: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"  Total: {all_results['total_hotels']}  "
                        f"OK: {all_results['successful_hotels']}  "
                        f"Failed: {all_results['failed_hotels']}  "
                        f"Auth failures: {all_results['authentication_failures']}\n")
                f.write(f"{'='*80}\n")

            logger.info(
                "Batch processing complete",
                total_hotels=all_results["total_hotels"],
                successful=all_results["successful_hotels"],
                failed=all_results["failed_hotels"],
                authentication_failures=all_results["authentication_failures"],
            )

            return all_results

        except Exception as e:
            logger.error(
                "Batch processing failed",
                error=str(e),
            )
            all_results["error"] = str(e)
            all_results["end_time"] = datetime.utcnow().isoformat()
            return all_results
