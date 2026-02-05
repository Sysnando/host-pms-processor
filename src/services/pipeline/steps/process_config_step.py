"""Step to process hotel configuration."""

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers import ConfigTransformer


class ProcessConfigStep(PipelineStep):
    """Fetch, transform, and upload hotel configuration."""

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
        super().__init__("ProcessConfig")
        self.host_api_client = host_api_client
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Process hotel configuration.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch config from Host PMS
            context.config_response = self.host_api_client.get_hotel_config(
                context.hotel_code
            )

            # Upload raw config to S3
            raw_upload = self.s3_manager.upload_raw(
                hotel_code=context.hotel_code,
                data_type="hotel-configs",
                data=context.config_response,
            )
            context.add_s3_upload("config_raw", raw_upload)

            # Transform config
            context.config_data, context.segments_collection = ConfigTransformer.transform(
                context.config_response
            )

            # Upload processed config to S3
            processed_upload = self.s3_manager.upload_processed(
                hotel_code=context.hotel_code,
                data_type="hotel-configs",
                data=context.config_data,
            )
            context.add_s3_upload("config_processed", processed_upload)

            # Register with ESB
            await self.esb_client.register_file(
                hotel_code=context.hotel_code,
                file_type="config",
                file_url=processed_upload["url"],
                file_key=processed_upload["key"],
                record_count=context.config_data.room_count,
            )

            # Add SQS message
            context.add_sqs_message("config", processed_upload["key"])

            # Store statistics
            context.stats["config"] = {
                "room_count": context.config_data.room_count,
            }

            self.logger.info(
                "Processed config successfully",
                hotel_code=context.hotel_code,
                room_count=context.config_data.room_count,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process config",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process config: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Config processing is optional - can continue without it.

        Returns:
            False
        """
        return False
