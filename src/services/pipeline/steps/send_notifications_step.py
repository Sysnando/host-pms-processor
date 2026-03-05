"""Step to send SQS notification to trigger import workflow."""

from src.aws import SQSManager
from src.config import settings
from src.services.pipeline import PipelineContext, PipelineStep


class SendNotificationsStep(PipelineStep):
    """Send final SQS message to trigger downstream import processing."""

    def __init__(self, sqs_manager: SQSManager):
        """Initialize the step.

        Args:
            sqs_manager: SQS manager for sending messages
        """
        super().__init__("SendNotifications")
        self.sqs_manager = sqs_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Send final SQS trigger message.

        Sends a single message to the processor queue with the hotel code
        to trigger the import workflow after all files are registered in ESB.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get hotel code for S3/SQS (should be uppercase)
            hotel_code_s3 = (
                settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or context.hotel_code
            ).strip().upper()

            self.logger.info(
                "Sending final SQS trigger message",
                hotel_code=context.hotel_code,
                hotel_code_s3=hotel_code_s3,
                message_group_id="HOST-CONNECTOR",
            )

            # Send single processor message to trigger import workflow
            sqs_result = self.sqs_manager.send_processor_message(
                hotel_code_s3=hotel_code_s3,
                message_group_id="HOST-CONNECTOR",
            )

            self.logger.info(
                "Sent SQS trigger message successfully",
                hotel_code=context.hotel_code,
                hotel_code_s3=hotel_code_s3,
                message_id=sqs_result["message_id"],
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to send SQS trigger message",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to send SQS trigger message: {str(e)}")
            return False

    def is_required(self) -> bool:
        """SQS notification is optional.

        Returns:
            False
        """
        return False
