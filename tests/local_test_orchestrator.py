"""Local test orchestrator for testing pipeline without AWS/Climber infrastructure.

This orchestrator extends HostPMSConnectorOrchestrator but replaces AWS and ESB
clients with mock versions that log operations and save files locally.

The real HostPMSAPIClient is still used to fetch actual data from Host PMS API.
There is no real-ESB path here by design — the real-ESB flow lives in the
production orchestrator (src/services/orchestration_service.py).
"""

from typing import Any, Optional

from structlog import get_logger

from src.aws.mock_s3_manager import MockS3Manager
from src.aws.mock_sqs_manager import MockSQSManager
from src.clients import HostPMSAPIClient
from src.clients.mock_esb_client import MockClimberESBClient
from src.services.orchestration_service import HostPMSConnectorOrchestrator
from src.services.pipeline import Pipeline
from src.services.pipeline.steps import (
    ProcessConfigStep,
    ProcessSegmentsStep,
    ProcessStatDailyStep,
    SendNotificationsStep
)
from tests.local_fetch_parameters_step import LocalFetchParametersStep, _parse_iso_date

logger = get_logger(__name__)


class LocalTestOrchestrator(HostPMSConnectorOrchestrator):
    """Test orchestrator that uses mock AWS/ESB clients for local testing.

    This allows running the full pipeline locally without:
    - S3 uploads (files saved to local directory instead)
    - ESB API calls (logged only — never POST/PUT to ESB)
    - SQS messages (logged instead of sent)

    The HostPMSAPIClient is still real, so it fetches actual data from Host PMS.
    """

    def __init__(
        self,
        output_dir: str = "./data_extracts",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        is_first_import: Optional[bool] = None,
    ):
        """Initialize the local test orchestrator with mock clients.

        Args:
            output_dir: Directory to save output files (default: ./data_extracts)
            from_date: Optional explicit start date (YYYY-MM-DD) for the import
                window. Bypasses the is-first-import branching in
                ``calculate_date_ranges``.
            to_date: Optional explicit end date (YYYY-MM-DD) for the import window.
            is_first_import: Optional explicit override for the ``is_first_import``
                flag on the pipeline context. If not provided, falls back to
                whatever the (mock) ESB returns from ``get_hotel_parameters``.
        """
        # Don't call super().__init__() - we want to replace the clients
        self.output_dir = output_dir

        # Validate date overrides at construction so bad input fails fast,
        # not deep inside the pipeline.
        if from_date:
            _parse_iso_date(from_date, "from_date")
        if to_date:
            _parse_iso_date(to_date, "to_date")
        if from_date and to_date and _parse_iso_date(from_date, "from_date") > _parse_iso_date(to_date, "to_date"):
            raise ValueError("from_date must be <= to_date")

        self._override_from_date = from_date
        self._override_to_date = to_date
        self._override_is_first_import = is_first_import

        self.esb_client = MockClimberESBClient()
        logger.info("Using MockClimberESBClient", mode="mock_esb")

        self.s3_manager = MockS3Manager(output_dir=output_dir)
        self.sqs_manager = MockSQSManager(output_dir=output_dir)

        # Captured per-hotel HostPMSAPIClient (built from getIntegration auth_id).
        # Reused by post-pipeline calls (e.g. fetch_stat_summary) so they don't
        # fall back to the .env default subscription key, which has its own
        # (low) quota and causes spurious 429s.
        self.hotel_api_clients: dict[str, HostPMSAPIClient] = {}

    def _build_pipeline(self, host_api_client: HostPMSAPIClient) -> Pipeline:
        """Build the ETL pipeline with all processing steps using mock clients.

        Args:
            host_api_client: Host PMS API client with hotel-specific credentials

        Returns:
            Configured pipeline ready to execute
        """
        steps = [
            # Step 1: Fetch import parameters — with optional explicit date-range override
            LocalFetchParametersStep(
                self.esb_client,
                from_date=self._override_from_date,
                to_date=self._override_to_date,
                is_first_import=self._override_is_first_import,
            ),
            # Step 2: Process hotel config (real API, mock S3)
            ProcessConfigStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 3: Process inventory grid (DEPRECATED)
            # ProcessInventoryGridStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 4: Process segments (mock S3/ESB)
            ProcessSegmentsStep(self.esb_client, self.s3_manager),
            # Step 5: Process StatDaily (real API, mock S3/ESB)
            ProcessStatDailyStep(host_api_client, self.esb_client, self.s3_manager),
            # Step 6: Update last import date (mocked)
            # UpdateImportDateStep(self.esb_client),
            # Step 7: Send SQS notifications (mocked)
            SendNotificationsStep(self.sqs_manager),
        ]

        return Pipeline(name="LocalTestPipeline", steps=steps)

    async def _resolve_host_api_client(self, hotel_code: str) -> HostPMSAPIClient:
        """Build a HostPMSAPIClient using the per-hotel auth_id from getIntegration.

        Mirrors the resolution logic in HostPMSConnectorOrchestrator.process_hotel,
        but executes it eagerly so we can capture the client for reuse by
        post-pipeline test helpers (e.g. fetch_stat_summary).
        """
        hotels = await self.esb_client.get_integration("BITZ")
        hotel_data = next((h for h in hotels if h.get("code") == hotel_code), None)
        if not hotel_data:
            raise RuntimeError(
                f"Hotel {hotel_code} not found in getIntegration response"
            )
        subscription_key = hotel_data.get("auth_id")
        if not subscription_key:
            raise RuntimeError(f"No auth_id found for hotel {hotel_code}")
        return HostPMSAPIClient(subscription_key=subscription_key)

    async def process_hotel(
        self,
        hotel_code: str,
        host_api_client: HostPMSAPIClient = None,
    ) -> dict[str, Any]:
        """Process a single hotel with hotel-specific output directory.

        Args:
            hotel_code: Hotel code to process
            host_api_client: Optional Host PMS API client with hotel-specific credentials

        Returns:
            Dictionary with processing results and statistics
        """
        # Set up hotel-specific directory for this processing run
        hotel_dir = self.s3_manager.get_hotel_directory(hotel_code)
        self.sqs_manager.set_hotel_directory(hotel_dir)

        # Resolve the per-hotel client up front so we can record it for reuse
        # by post-pipeline helpers (StatSummary etc.). Without this they would
        # default to .env credentials and burn a separate (low) quota.
        if host_api_client is None:
            host_api_client = await self._resolve_host_api_client(hotel_code)
        self.hotel_api_clients[hotel_code] = host_api_client

        return await super().process_hotel(hotel_code, host_api_client)
