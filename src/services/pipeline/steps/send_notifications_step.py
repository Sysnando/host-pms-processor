"""Step to send SQS notifications."""

from src.aws import SQSManager
from src.services.pipeline import PipelineContext, PipelineStep


class SendNotificationsStep(PipelineStep):
    """Send SQS messages to trigger downstream processing."""

    def __init__(self, sqs_manager: SQSManager):
        """Initialize the step.

        Args:
            sqs_manager: SQS manager for sending messages
        """
        super().__init__("SendNotifications")
        self.sqs_manager = sqs_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Send SQS notifications.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(
                "Sending SQS messages",
                hotel_code=context.hotel_code,
                message_count=len(context.sqs_messages),
            )

            for message in context.sqs_messages:
                sqs_result = self.sqs_manager.send_message(
                    hotel_code=message["hotel_code"],
                    file_type=message["file_type"],
                    file_key=message["file_key"],
                )
                message["sqs_message_id"] = sqs_result["message_id"]

            self.logger.info(
                "Sent SQS messages successfully",
                hotel_code=context.hotel_code,
                message_count=len(context.sqs_messages),
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to send SQS messages",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to send SQS messages: {str(e)}")
            return False

    def is_required(self) -> bool:
        """SQS notification is optional.

        Returns:
            False
        """
        return False
