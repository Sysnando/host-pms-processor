"""Step to process room inventory."""

from src.aws import S3Manager
from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers import ConfigTransformer


class ProcessInventoryStep(PipelineStep):
    """Extract and upload room inventory from config."""

    def __init__(
        self,
        esb_client: ClimberESBClient,
        s3_manager: S3Manager,
    ):
        """Initialize the step.

        Args:
            esb_client: Climber ESB API client
            s3_manager: S3 manager for uploads
        """
        super().__init__("ProcessInventory")
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Process room inventory.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        # Check if config response is available
        if context.config_response is None:
            self.logger.warning(
                "No config response available, skipping inventory",
                hotel_code=context.hotel_code,
            )
            return False

        try:
            # Extract room inventory from config
            context.room_inventory = ConfigTransformer.get_room_inventory(
                context.config_response
            )

            # Upload processed inventory to S3
            processed_upload = self.s3_manager.upload_processed(
                hotel_code=context.hotel_code,
                data_type="inventory",
                data=context.room_inventory,
            )
            context.add_s3_upload("inventory_processed", processed_upload)

            # Register with ESB
            await self.esb_client.register_file(
                hotel_code=context.hotel_code,
                file_type="inventory",
                file_url=processed_upload["url"],
                file_key=processed_upload["key"],
                record_count=len(context.room_inventory.room_inventory),
            )

            # Add SQS message
            context.add_sqs_message("inventory", processed_upload["key"])

            # Store statistics
            context.stats["inventory"] = {
                "room_count": len(context.room_inventory.room_inventory),
            }

            self.logger.info(
                "Processed inventory successfully",
                hotel_code=context.hotel_code,
                room_count=len(context.room_inventory.room_inventory),
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process inventory",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process inventory: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Inventory processing is optional.

        Returns:
            False
        """
        return False
