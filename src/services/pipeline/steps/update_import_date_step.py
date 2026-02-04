"""Step to update last import date in ESB."""

from datetime import datetime

from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext, PipelineStep


class UpdateImportDateStep(PipelineStep):
    """Update the last import date in Climber ESB."""

    def __init__(self, esb_client: ClimberESBClient):
        """Initialize the step.

        Args:
            esb_client: Climber ESB API client
        """
        super().__init__("UpdateImportDate")
        self.esb_client = esb_client

    async def execute(self, context: PipelineContext) -> bool:
        """Update import date in ESB.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            await self.esb_client.update_import_date(
                hotel_code=context.hotel_code,
                last_import_date=datetime.utcnow().isoformat(),
            )

            self.logger.info(
                "Updated import date",
                hotel_code=context.hotel_code,
            )

            return True

        except Exception as e:
            self.logger.warning(
                "Failed to update import date",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to update import date: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Import date update is optional.

        Returns:
            False
        """
        return False
