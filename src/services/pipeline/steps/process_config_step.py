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

        Fetches hotel config from Host PMS API and transforms it to extract
        segments and room inventory. Uploads inventory to hotel-configs bucket
        and registers with ESB.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch config from Host PMS
            self.logger.info(
                "Fetching hotel config from Host PMS API",
                hotel_code=context.hotel_code,
            )
            context.config_response = self.host_api_client.get_hotel_config(
                context.hotel_code
            )

            # Transform config to extract segments
            # Config data and segments are stored in context for other steps
            context.config_data, context.segments_collection = ConfigTransformer.transform(
                context.config_response
            )

            # Extract room inventory from config (CATEGORY items with Inventory field)
            # This replaces the deprecated InventoryGrid API call
            self.logger.info(
                "Extracting room inventory from config",
                hotel_code=context.hotel_code,
            )

            from datetime import datetime
            execution_date = datetime.utcnow().date()

            context.room_inventory = ConfigTransformer.get_room_inventory(
                context.config_response,
                execution_date=execution_date,
            )

            # Upload raw config data
            if context.config_response:
                raw_upload = self.s3_manager.upload_raw(
                    hotel_code=context.hotel_code,
                    data_type="hotel-configs",
                    data=context.config_response,
                )
                context.add_s3_upload("config_raw", raw_upload)

            # Upload processed inventory to hotel-configs bucket
            if context.room_inventory and len(context.room_inventory.room_inventory) > 0:
                processed_upload = self.s3_manager.upload_processed(
                    hotel_code=context.hotel_code,
                    data_type="hotel-configs",
                    data=context.room_inventory,
                )
                context.add_s3_upload("inventory_processed", processed_upload)

                # Register with ESB
                await self.esb_client.register_file(
                    hotel_code=context.hotel_code,
                    file_type="hotel-configs",
                    file_url=processed_upload["url"],
                    file_key=processed_upload["key"],
                    record_count=len(context.room_inventory.room_inventory),
                    is_first_import=context.is_first_import,
                )

                self.logger.info(
                    "Uploaded and registered room inventory",
                    hotel_code=context.hotel_code,
                    room_count=len(context.room_inventory.room_inventory),
                )

            # Store statistics
            context.stats["config"] = {
                "room_count": context.config_data.room_count,
                "segments_extracted": True,
                "inventory_items": len(context.room_inventory.room_inventory) if context.room_inventory else 0,
            }

            self.logger.info(
                "Processed config successfully",
                hotel_code=context.hotel_code,
                room_count=context.config_data.room_count,
                inventory_items=len(context.room_inventory.room_inventory) if context.room_inventory else 0,
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
