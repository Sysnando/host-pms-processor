"""Base class for pipeline steps."""

from abc import ABC, abstractmethod

from structlog import get_logger

logger = get_logger(__name__)


class PipelineStep(ABC):
    """Abstract base class for pipeline steps.

    Each step should:
    1. Implement execute() method
    2. Read data from context
    3. Perform its work
    4. Write results back to context
    5. Return success boolean
    """

    def __init__(self, name: str | None = None):
        """Initialize the pipeline step.

        Args:
            name: Optional custom name for the step. Defaults to class name.
        """
        self.name = name or self.__class__.__name__
        self.logger = logger.bind(step=self.name)

    @abstractmethod
    async def execute(self, context: "PipelineContext") -> bool:
        """Execute the pipeline step.

        Args:
            context: Pipeline context containing shared data

        Returns:
            True if step succeeded, False if failed
        """
        pass

    async def run(self, context: "PipelineContext") -> bool:
        """Run the step with error handling and logging.

        Args:
            context: Pipeline context

        Returns:
            True if step succeeded, False if failed
        """
        self.logger.info("Step starting", hotel_code=context.hotel_code)

        try:
            success = await self.execute(context)

            if success:
                self.logger.info("Step completed successfully", hotel_code=context.hotel_code)
            else:
                self.logger.warning("Step completed with failure", hotel_code=context.hotel_code)

            return success

        except Exception as e:
            self.logger.error(
                "Step failed with exception",
                hotel_code=context.hotel_code,
                error=str(e),
                exc_info=True,
            )
            context.add_error(self.name, str(e))
            return False

    def is_required(self) -> bool:
        """Check if this step is required for pipeline success.

        Returns:
            True if step failure should stop pipeline, False if optional
        """
        return True

    def get_name(self) -> str:
        """Get the step name.

        Returns:
            Step name
        """
        return self.name
