"""Step to fetch import parameters from ESB."""

from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.utils.date_calculator import calculate_date_ranges


class FetchParametersStep(PipelineStep):
    """Fetch import parameters (last import date) from Climber ESB."""

    def __init__(self, esb_client: ClimberESBClient):
        """Initialize the step.

        Args:
            esb_client: Climber ESB API client
        """
        super().__init__("FetchParameters")
        self.esb_client = esb_client

    async def execute(self, context: PipelineContext) -> bool:
        """Fetch import parameters from ESB.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            parameters = await self.esb_client.get_hotel_parameters(context.hotel_code)
            # context.last_import_date = parameters.get("lastImportDate")
            context.min_import_date = parameters.get("minImportDate")
            context.max_import_date = parameters.get("maxImportDate")
            # Set is_first_import based on whether lastImportDate is null/empty
            context.is_first_import = not context.last_import_date

            self.logger.info(
                "Fetched import parameters",
                hotel_code=context.hotel_code,
                last_import_date=context.last_import_date,
                min_import_date=context.min_import_date,
                max_import_date=context.max_import_date,
                is_first_import=context.is_first_import,
            )

            # Calculate date ranges based on ESB parameters
            (
                reservation_from_date,
                stat_daily_start,
                stat_daily_end,
                inventory_from,
                inventory_to,
            ) = calculate_date_ranges(
                context.last_import_date,
                context.min_import_date,
                context.max_import_date,
            )

            # Store calculated dates in context
            context.calculated_reservation_from_date = reservation_from_date
            context.calculated_stat_daily_start = stat_daily_start
            context.calculated_stat_daily_end = stat_daily_end
            context.calculated_inventory_from = inventory_from
            context.calculated_inventory_to = inventory_to

            self.logger.info(
                "Calculated date ranges",
                hotel_code=context.hotel_code,
                reservation_from=reservation_from_date,
                stat_daily_start=stat_daily_start,
                stat_daily_end=stat_daily_end,
                inventory_from=inventory_from,
                inventory_to=inventory_to,
                is_first_import=(context.last_import_date is None),
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to fetch import parameters",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to fetch parameters: {str(e)}")
            return False

    def is_required(self) -> bool:
        """This step is required - cannot proceed without import parameters.

        Returns:
            True
        """
        return True
