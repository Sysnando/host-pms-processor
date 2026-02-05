"""Main orchestrator service for the Host PMS connector ETL pipeline."""

from datetime import datetime
from typing import Any

from structlog import get_logger

from src.aws import S3Manager, SQSManager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import Pipeline, PipelineContext
from src.services.pipeline.steps import (
    FetchParametersStep,
    ProcessConfigStep,
    ProcessInventoryStep,
    ProcessReservationsStep,
    ProcessSegmentsStep,
    ProcessStatDailyStep,
    SendNotificationsStep,
    UpdateImportDateStep,
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

    def __init__(self):
        """Initialize the orchestrator with all required services."""
        self.esb_client = ClimberESBClient()
        self.host_api_client = HostPMSAPIClient()
        self.s3_manager = S3Manager()
        self.sqs_manager = SQSManager()

    def _build_pipeline(self) -> Pipeline:
        """Build the ETL pipeline with all processing steps.

        Returns:
            Configured pipeline ready to execute
        """
        steps = [
            # Step 1: Fetch import parameters (required)
            FetchParametersStep(self.esb_client),
            # Step 2: Process hotel config (optional)
            ProcessConfigStep(self.host_api_client, self.esb_client, self.s3_manager),
            # Step 3: Process room inventory (optional)
            ProcessInventoryStep(self.esb_client, self.s3_manager),
            # Step 4: Process segments (optional)
            ProcessSegmentsStep(self.esb_client, self.s3_manager),
            # Step 5: Process reservations (optional)
            ProcessReservationsStep(self.host_api_client, self.esb_client, self.s3_manager),
            # Step 6: Process StatDaily and update invoices (optional)
            ProcessStatDailyStep(self.host_api_client, self.esb_client, self.s3_manager),
            # Step 7: Update last import date (optional)
            UpdateImportDateStep(self.esb_client),
            # Step 8: Send SQS notifications (optional)
            SendNotificationsStep(self.sqs_manager),
        ]

        return Pipeline(name="HostPMSConnectorPipeline", steps=steps)

    async def process_hotel(
        self,
        hotel_code: str,
    ) -> dict[str, Any]:
        """Process a single hotel through the ETL pipeline.

        This method uses a pipeline pattern to execute discrete processing steps.
        Each step is independent, testable, and can fail gracefully without
        affecting other steps.

        Args:
            hotel_code: Hotel code to process

        Returns:
            Dictionary with processing results and statistics

        Raises:
            OrchestrationError: If critical processing fails
        """
        logger.info(
            "Starting hotel processing",
            hotel_code=hotel_code,
        )

        try:
            # Create pipeline context
            context = PipelineContext(hotel_code=hotel_code)

            # Build and execute pipeline
            pipeline = self._build_pipeline()
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

    async def process_all_hotels(self) -> dict[str, Any]:
        """Process all configured hotels through the ETL pipeline.

        Returns:
            Dictionary with aggregated results for all hotels
        """
        logger.info("Starting batch processing of all hotels")

        all_results = {
            "total_hotels": 0,
            "successful_hotels": 0,
            "failed_hotels": 0,
            "hotels": [],
            "start_time": datetime.utcnow().isoformat(),
        }

        try:
            # Step 1: Get list of configured hotels from ESB
            logger.info("Fetching hotel list from ESB")
            hotels = await self.esb_client.get_hotels()
            all_results["total_hotels"] = len(hotels)

            logger.info("Successfully fetched hotels", hotel_count=len(hotels))

            # Step 2: Process each hotel
            for hotel in hotels:
                hotel_code = hotel.get("code") or hotel.get("hotelCode")

                if not hotel_code:
                    logger.warning("Skipping hotel with no code", hotel=hotel)
                    continue

                try:
                    result = await self.process_hotel(hotel_code)
                    all_results["hotels"].append(result)

                    if result["success"]:
                        all_results["successful_hotels"] += 1
                    else:
                        all_results["failed_hotels"] += 1

                except Exception as e:
                    logger.error(
                        "Failed to process hotel",
                        hotel_code=hotel_code,
                        error=str(e),
                    )
                    all_results["hotels"].append(
                        {
                            "hotel_code": hotel_code,
                            "success": False,
                            "errors": [str(e)],
                        }
                    )
                    all_results["failed_hotels"] += 1

            all_results["end_time"] = datetime.utcnow().isoformat()

            logger.info(
                "Batch processing complete",
                total_hotels=all_results["total_hotels"],
                successful=all_results["successful_hotels"],
                failed=all_results["failed_hotels"],
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
