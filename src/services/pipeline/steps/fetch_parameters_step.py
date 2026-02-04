"""Step to fetch import parameters from ESB."""

from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext, PipelineStep


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
            context.last_import_date = parameters.get("lastImportDate")

            self.logger.info(
                "Fetched import parameters",
                hotel_code=context.hotel_code,
                last_import_date=context.last_import_date,
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
