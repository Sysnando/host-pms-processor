"""Pipeline executor for orchestrating ETL steps."""

from typing import Any

from structlog import get_logger

from .base_step import PipelineStep
from .context import PipelineContext

logger = get_logger(__name__)


class Pipeline:
    """Pipeline for executing a sequence of processing steps.

    The pipeline:
    1. Executes steps in order
    2. Passes context between steps
    3. Handles errors gracefully
    4. Supports optional/required steps
    5. Collects results and statistics
    """

    def __init__(self, name: str, steps: list[PipelineStep]):
        """Initialize the pipeline.

        Args:
            name: Pipeline name for logging
            steps: List of pipeline steps to execute in order
        """
        self.name = name
        self.steps = steps
        self.logger = logger.bind(pipeline=name)

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the pipeline.

        Args:
            context: Pipeline context

        Returns:
            Updated context with results
        """
        self.logger.info(
            "Pipeline starting",
            hotel_code=context.hotel_code,
            step_count=len(self.steps),
        )

        successful_steps = 0
        failed_steps = 0

        for step in self.steps:
            step_name = step.get_name()

            self.logger.info(
                "Executing step",
                hotel_code=context.hotel_code,
                step=step_name,
            )

            try:
                success = await step.run(context)

                if success:
                    successful_steps += 1
                else:
                    failed_steps += 1

                    # If step is required and failed, stop pipeline
                    if step.is_required():
                        self.logger.error(
                            "Required step failed, stopping pipeline",
                            hotel_code=context.hotel_code,
                            step=step_name,
                        )
                        break
                    else:
                        self.logger.warning(
                            "Optional step failed, continuing pipeline",
                            hotel_code=context.hotel_code,
                            step=step_name,
                        )

            except Exception as e:
                self.logger.error(
                    "Step raised unexpected exception",
                    hotel_code=context.hotel_code,
                    step=step_name,
                    error=str(e),
                    exc_info=True,
                )
                context.add_error(step_name, f"Unexpected exception: {str(e)}")
                failed_steps += 1

                # If step is required and failed, stop pipeline
                if step.is_required():
                    break

        # Mark success if no errors
        context.success = not context.has_errors()

        # Add pipeline statistics
        context.stats["pipeline"] = {
            "name": self.name,
            "total_steps": len(self.steps),
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
        }

        self.logger.info(
            "Pipeline completed",
            hotel_code=context.hotel_code,
            success=context.success,
            successful_steps=successful_steps,
            failed_steps=failed_steps,
        )

        return context

    def add_step(self, step: PipelineStep) -> "Pipeline":
        """Add a step to the pipeline.

        Args:
            step: Pipeline step to add

        Returns:
            Self for method chaining
        """
        self.steps.append(step)
        return self

    def get_step_names(self) -> list[str]:
        """Get list of all step names in the pipeline.

        Returns:
            List of step names
        """
        return [step.get_name() for step in self.steps]
