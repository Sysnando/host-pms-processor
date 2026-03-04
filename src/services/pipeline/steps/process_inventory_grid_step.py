"""Step to process room inventory from InventoryGrid API.

This step fetches inventory data from the Host PMS InventoryGrid API,
which provides detailed rate-based inventory information, transforms it
to Climber format, and uploads to hotel-configs buckets.
"""

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers.inventory_grid_transformer import InventoryGridTransformer


class ProcessInventoryGridStep(PipelineStep):
    """Fetch inventory from InventoryGrid API for all rate codes.

    This step replaces the old inventory extraction from config.
    InventoryGrid provides more accurate and detailed inventory data
    per rate code.
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
        super().__init__("ProcessInventoryGrid")
        self.host_api_client = host_api_client
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Process inventory from InventoryGrid API.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        # Check if config response is available (needed for rate codes)
        if context.config_response is None:
            self.logger.warning(
                "No config response available, skipping inventory grid",
                hotel_code=context.hotel_code,
            )
            return False

        # Check if calculated date ranges are available
        if not context.calculated_inventory_from or not context.calculated_inventory_to:
            self.logger.warning(
                "No calculated inventory date ranges available, skipping inventory grid",
                hotel_code=context.hotel_code,
            )
            return False

        try:
            from_date = context.calculated_inventory_from
            to_date = context.calculated_inventory_to

            self.logger.info(
                "Fetching inventory grid from Host PMS API",
                hotel_code=context.hotel_code,
                from_date=from_date,
                to_date=to_date,
            )

            # Fetch inventory for all rate codes
            inventory_response = await self.host_api_client.get_inventory_all_rates(
                config_response=context.config_response,
                from_date=from_date,
                to_date=to_date,
            )

            room_inventories = inventory_response.get("roomInventories", [])

            self.logger.info(
                "Fetched inventory grid",
                hotel_code=context.hotel_code,
                total_items=len(room_inventories),
            )

            # Upload raw inventory data
            if room_inventories:
                raw_upload = self.s3_manager.upload_raw(
                    hotel_code=context.hotel_code,
                    data_type="hotel-configs",
                    data=inventory_response,
                )
                context.add_s3_upload("inventory_grid_raw", raw_upload)

                # Transform to Climber format
                self.logger.info(
                    "Transforming inventory grid to Climber format",
                    hotel_code=context.hotel_code,
                )
                room_inventory_data = InventoryGridTransformer.transform(inventory_response)

                # Upload processed inventory in Climber format
                processed_upload = self.s3_manager.upload_processed(
                    hotel_code=context.hotel_code,
                    data_type="hotel-configs",
                    data=room_inventory_data,
                )
                context.add_s3_upload("inventory_grid_processed", processed_upload)

                # Register with ESB
                await self.esb_client.register_file(
                    hotel_code=context.hotel_code,
                    file_type="hotel-configs",
                    file_url=processed_upload["url"],
                    file_key=processed_upload["key"],
                    record_count=len(room_inventory_data.room_inventory),
                )

                # Add SQS message
                context.add_sqs_message("inventory", processed_upload["key"])

                # Store statistics
                context.stats["inventory_grid"] = {
                    "item_count": len(room_inventories),
                    "from_date": from_date,
                    "to_date": to_date,
                }

                self.logger.info(
                    "Processed inventory grid successfully",
                    hotel_code=context.hotel_code,
                    item_count=len(room_inventories),
                )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process inventory grid",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process inventory grid: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Inventory grid processing is optional.

        Returns:
            False
        """
        return False
