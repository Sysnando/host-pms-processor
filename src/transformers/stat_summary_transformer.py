"""Transformer for StatSummary data.

This transformer is used only for testing/validation purposes.
It performs no calculations or validations - just transforms the API
response into a clean format for database storage.
"""

from typing import Any

from structlog import get_logger

from src.models.host.stat_summary import StatSummaryRecord

logger = get_logger(__name__)


class StatSummaryTransformer:
    """Transform StatSummary API responses for database storage."""

    @staticmethod
    def transform(
        stat_summary_records: list[dict[str, Any]],
    ) -> list[StatSummaryRecord]:
        """Transform StatSummary records from API response.

        This is a simple pass-through transformer that validates the data
        against the Pydantic model but performs no calculations or aggregations.

        Args:
            stat_summary_records: List of StatSummary records from API

        Returns:
            List of validated StatSummaryRecord objects
        """
        logger.info(
            "Transforming StatSummary records",
            total_records=len(stat_summary_records),
        )

        transformed_records = []
        failed_count = 0

        for record in stat_summary_records:
            try:
                stat_record = StatSummaryRecord(**record)
                transformed_records.append(stat_record)
            except Exception as e:
                logger.warning(
                    "Failed to transform StatSummary record",
                    record=record,
                    error=str(e),
                )
                failed_count += 1

        logger.info(
            "StatSummary transformation complete",
            total_records=len(stat_summary_records),
            transformed=len(transformed_records),
            failed=failed_count,
        )

        return transformed_records
