"""Local override for FetchParametersStep that supports explicit date ranges.

Production code derives the data window from ESB parameters via
`calculate_date_ranges`, which branches on whether `lastImportDate` is None
(first import vs incremental). For local testing we want to be able to pin
an arbitrary window without depending on that branch.

If `from_date` / `to_date` are provided, this step skips `calculate_date_ranges`
entirely and writes the override values directly into the pipeline context.
Otherwise it behaves exactly like the standard `FetchParametersStep`.
"""

from datetime import date, datetime
from typing import Optional

from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext
from src.services.pipeline.steps.fetch_parameters_step import FetchParametersStep
from src.utils.date_calculator import calculate_date_ranges


def _parse_iso_date(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(
            f"{field_name} must be YYYY-MM-DD (got {value!r}): {e}"
        ) from e


class LocalFetchParametersStep(FetchParametersStep):
    """FetchParametersStep that honors explicit date-range overrides."""

    def __init__(
        self,
        esb_client: ClimberESBClient,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        is_first_import: Optional[bool] = None,
    ):
        super().__init__(esb_client)
        self._override_from = from_date
        self._override_to = to_date
        self._override_is_first_import = is_first_import

        if self._override_from:
            _parse_iso_date(self._override_from, "FROM_DATE")
        if self._override_to:
            _parse_iso_date(self._override_to, "TO_DATE")
        if self._override_from and self._override_to:
            if _parse_iso_date(self._override_from, "FROM_DATE") > _parse_iso_date(
                self._override_to, "TO_DATE"
            ):
                raise ValueError("FROM_DATE must be <= TO_DATE")

    async def execute(self, context: PipelineContext) -> bool:
        if not (self._override_from or self._override_to):
            return await super().execute(context)

        try:
            parameters = await self.esb_client.get_hotel_parameters(context.hotel_code)
            context.last_import_date = parameters.get("lastImportDate")
            context.min_import_date = parameters.get("minImportDate")
            context.max_import_date = parameters.get("maxImportDate")

            if self._override_is_first_import is not None:
                context.is_first_import = self._override_is_first_import
            else:
                context.is_first_import = parameters.get("isFirstImport", False)

            from_date_str = self._override_from
            to_date_str = self._override_to

            if not from_date_str or not to_date_str:
                (
                    fallback_reservation_from,
                    fallback_stat_start,
                    fallback_stat_end,
                    _,
                    _,
                ) = calculate_date_ranges(
                    context.last_import_date,
                    context.min_import_date,
                    context.max_import_date,
                )
                from_date_str = from_date_str or fallback_stat_start
                to_date_str = to_date_str or fallback_stat_end

            reservation_from = f"{from_date_str}T00:00:00Z"

            context.calculated_reservation_from_date = reservation_from
            context.calculated_stat_daily_start = from_date_str
            context.calculated_stat_daily_end = to_date_str
            context.calculated_inventory_from = from_date_str
            context.calculated_inventory_to = to_date_str

            self.logger.info(
                "Using LOCAL date-range override (skipping is-first-import branching)",
                hotel_code=context.hotel_code,
                from_date=from_date_str,
                to_date=to_date_str,
                is_first_import=context.is_first_import,
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to apply local date-range override",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to fetch parameters: {str(e)}")
            return False
