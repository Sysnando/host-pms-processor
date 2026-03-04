"""Local test orchestrator for testing pipeline without AWS/Climber infrastructure.

This orchestrator extends HostPMSConnectorOrchestrator but replaces AWS and ESB
clients with mock versions that log operations and save files locally.

The real HostPMSAPIClient is still used to fetch actual data from Host PMS API.
"""

from typing import Any

from src.aws.mock_s3_manager import MockS3Manager
from src.aws.mock_sqs_manager import MockSQSManager
from src.clients import HostPMSAPIClient
from src.clients.mock_esb_client import MockClimberESBClient
from src.services.orchestration_service import HostPMSConnectorOrchestrator
from src.services.pipeline import Pipeline
from src.services.pipeline.steps import (
    FetchParametersStep,
    ProcessConfigStep,
    ProcessInventoryGridStep,
    ProcessSegmentsStep,
    ProcessStatDailyStep,
    SendNotificationsStep,
    UpdateImportDateStep,
)


class LocalTestOrchestrator(HostPMSConnectorOrchestrator):
    """Test orchestrator that uses mock AWS/ESB clients for local testing.

    This allows running the full pipeline locally without:
    - S3 uploads (files saved to local directory instead)
    - ESB API calls (mocked with test data)
    - SQS messages (logged instead of sent)

    The HostPMSAPIClient is still real, so it fetches actual data from Host PMS.
    """

    def __init__(self, output_dir: str = "./data_extracts"):
        """Initialize the local test orchestrator with mock clients.

        Args:
            output_dir: Directory to save output files (default: ./data_extract)
        """
        # Don't call super().__init__() - we want to replace the clients
        self.output_dir = output_dir

        # Mock clients (log only, save files locally)
        self.esb_client = MockClimberESBClient()
        self.s3_manager = MockS3Manager(output_dir=output_dir)
        self.sqs_manager = MockSQSManager(output_dir=output_dir)

        # Real client (fetches actual data from Host PMS)
        self.host_api_client = HostPMSAPIClient()

    def _build_pipeline(self) -> Pipeline:
        """Build the ETL pipeline with all processing steps using mock clients.

        Returns:
            Configured pipeline ready to execute
        """
        steps = [
            # Step 1: Fetch import parameters (mocked)
            FetchParametersStep(self.esb_client),
            # Step 2: Process hotel config (real API, no upload)
            ProcessConfigStep(self.host_api_client, self.esb_client, self.s3_manager),
            # Step 3: Process inventory grid (real API, mock S3/ESB)
            ProcessInventoryGridStep(
                self.host_api_client, self.esb_client, self.s3_manager
            ),
            # Step 4: Process segments (mock S3/ESB)
            ProcessSegmentsStep(self.esb_client, self.s3_manager),
            # Step 5: Process StatDaily (real API, mock S3/ESB)
            ProcessStatDailyStep(self.host_api_client, self.esb_client, self.s3_manager),
            # Step 6: Update last import date (mocked)
            UpdateImportDateStep(self.esb_client),
            # Step 7: Send SQS notifications (mocked)
            SendNotificationsStep(self.sqs_manager),
        ]

        return Pipeline(name="LocalTestPipeline", steps=steps)

    async def process_hotel(self, hotel_code: str) -> dict[str, Any]:
        """Process a single hotel with hotel-specific output directory.

        Args:
            hotel_code: Hotel code to process

        Returns:
            Dictionary with processing results and statistics
        """
        # Set up hotel-specific directory for this processing run
        hotel_dir = self.s3_manager.get_hotel_directory(hotel_code)
        self.sqs_manager.set_hotel_directory(hotel_dir)

        # Call parent process_hotel method
        return await super().process_hotel(hotel_code)
