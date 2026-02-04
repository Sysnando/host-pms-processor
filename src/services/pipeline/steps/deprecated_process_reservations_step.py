"""DEPRECATED: Step to process reservations.

This step is deprecated. StatDaily data is now the primary source for reservations.
Use ProcessStatDailyStep which converts StatDaily data to reservations using
StatDailyToReservationTransformer.

This file is kept for reference only.
"""

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers import ReservationTransformer


class DeprecatedProcessReservationsStep(PipelineStep):
    """DEPRECATED: Fetch, transform, and upload reservations.

    This step is no longer used. StatDaily is the primary data source.
    """

    def __init__(
        self,
        host_api_client: HostPMSAPIClient,
        esb_client: ClimberESBClient,
        s3_manager: S3Manager,
    ):
        """Initialize the step.

        Args:
            host_api_client: Host PMS API client
            esb_client: Climber ESB API client
            s3_manager: S3 manager for uploads
        """
        super().__init__("ProcessReservations")
        self.host_api_client = host_api_client
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Process reservations.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch reservations from Host PMS
            context.reservations_response = await self.host_api_client.get_reservations(
                hotel_code=context.hotel_code,
                update_from=context.last_import_date,
            )

            # Upload raw reservations to S3
            raw_upload = self.s3_manager.upload_raw(
                hotel_code=context.hotel_code,
                data_type="reservations",
                data=context.reservations_response,
            )
            context.add_s3_upload("reservations_raw", raw_upload)

            # Extract reservations data
            if isinstance(context.reservations_response, dict):
                reservations_data = context.reservations_response.get("Reservations", [])
            else:
                reservations_data = context.reservations_response.reservations

            # Transform reservations
            context.reservations_collection, _, _, _ = ReservationTransformer.transform_batch(
                reservations_data,
                hotel_code=context.hotel_code,
            )

            # Upload processed reservations to S3
            processed_upload = self.s3_manager.upload_processed(
                hotel_code=context.hotel_code,
                data_type="reservations",
                data=context.reservations_collection,
            )
            context.add_s3_upload("reservations_processed", processed_upload)

            # Register with ESB
            await self.esb_client.register_file(
                hotel_code=context.hotel_code,
                file_type="reservation",
                file_url=processed_upload["url"],
                file_key=processed_upload["key"],
                record_count=context.reservations_collection.total_count,
            )

            # Add SQS message
            context.add_sqs_message("reservations", processed_upload["key"])

            # Store statistics
            context.stats["reservations"] = {
                "record_count": context.reservations_collection.total_count,
            }

            self.logger.info(
                "Processed reservations successfully",
                hotel_code=context.hotel_code,
                record_count=context.reservations_collection.total_count,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process reservations",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process reservations: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Reservations processing is optional.

        Returns:
            False
        """
        return False
