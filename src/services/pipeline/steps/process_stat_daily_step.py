"""Step to process StatDaily data and convert it to reservations.

This step is the primary source for reservation data. It fetches StatDaily records
from the Host PMS API and transforms them into Climber reservations using the
StatDailyToReservationTransformer.
"""

import asyncio
from datetime import datetime, timedelta

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.config import settings
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers.stat_daily_to_reservation_transformer import (
    StatDailyToReservationTransformer,
)


class ProcessStatDailyStep(PipelineStep):
    """Fetch StatDaily data and convert to Climber reservations.

    This step replaces the old ProcessReservationsStep. StatDaily is now
    the primary source for reservation data as it contains complete
    occupancy, revenue, and segment information.
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
        super().__init__("ProcessStatDaily")
        self.host_api_client = host_api_client
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    def _create_date_chunks(self, start_date, end_date, months=4):
        """Split date range into chunks of specified months for memory-efficient processing.

        Args:
            start_date: Start date of the range
            end_date: End date of the range
            months: Number of months per chunk (default: 4)

        Returns:
            List of tuples (chunk_start_date, chunk_end_date)
        """
        chunks = []
        current_start = start_date

        while current_start <= end_date:
            # Calculate chunk end (~4 months or 120 days)
            current_end = min(
                current_start + timedelta(days=120),
                end_date
            )
            chunks.append((current_start, current_end))
            current_start = current_end + timedelta(days=1)

        return chunks

    async def _fetch_date_async(
        self,
        date: any,
        hotel_code: str,
    ) -> list:
        """Fetch StatDaily for a single date asynchronously.

        Args:
            date: Date to fetch
            hotel_code: Hotel code for logging

        Returns:
            List of StatDaily records for the date, or empty list if fetch fails
        """
        try:
            date_str = date.isoformat()
            stat_daily_response = await self.host_api_client.get_stat_daily_async(
                hotel_date_filter=date_str,
                hotel_code=hotel_code
            )

            if isinstance(stat_daily_response, list):
                return stat_daily_response
            return []

        except Exception as e:
            self.logger.warning(
                "Failed to fetch StatDaily for date",
                hotel_code=hotel_code,
                date=date.isoformat(),
                error=str(e),
            )
            # Return empty list - continue with other dates even if one fails
            return []

    async def _process_chunk(
        self,
        context: PipelineContext,
        chunk_start: any,
        chunk_end: any,
        hotel_local_time: any,
        chunk_index: int = None,
        total_chunks: int = None,
    ) -> dict:
        """Process a single date range chunk: fetch, transform, upload, register.

        Args:
            context: Pipeline context
            chunk_start: Start date of chunk
            chunk_end: End date of chunk
            hotel_local_time: Hotel local time for record_date calculation
            chunk_index: Index of current chunk (for logging)
            total_chunks: Total number of chunks (for logging)

        Returns:
            Dictionary with stats: {raw_count, reservation_count}
        """
        chunk_label = f"chunk {chunk_index}/{total_chunks}" if chunk_index else "full range"

        self.logger.info(
            f"Processing {chunk_label}",
            hotel_code=context.hotel_code,
            start_date=chunk_start.isoformat(),
            end_date=chunk_end.isoformat(),
        )

        # Fetch StatDaily for this chunk in parallel
        # Build list of all dates in the chunk
        dates = []
        current_date = chunk_start
        while current_date <= chunk_end:
            dates.append(current_date)
            current_date += timedelta(days=1)

        self.logger.info(
            f"Fetching StatDaily for {len(dates)} dates in parallel for {chunk_label}",
            hotel_code=context.hotel_code,
            date_count=len(dates),
        )

        # Create semaphore to limit concurrent date requests (10 at a time per hotel)
        # This prevents overwhelming the API while still getting massive parallelization
        date_semaphore = asyncio.Semaphore(10)

        async def fetch_with_semaphore(date):
            async with date_semaphore:
                return await self._fetch_date_async(date, context.hotel_code)

        # Fetch all dates in parallel (max 10 concurrent per hotel)
        tasks = [fetch_with_semaphore(date) for date in dates]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Combine all results
        chunk_records = []
        for date_records in results:
            if date_records:  # Skip empty lists from failed fetches
                chunk_records.extend(date_records)

        self.logger.info(
            f"Fetched StatDaily data for {chunk_label}",
            hotel_code=context.hotel_code,
            total_records=len(chunk_records),
        )

        if not chunk_records:
            self.logger.warning(
                f"No records found for {chunk_label}, skipping",
                hotel_code=context.hotel_code,
            )
            return {"raw_count": 0, "reservation_count": 0}

        # Generate fresh timestamp for this chunk to ensure unique filenames
        chunk_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Upload raw StatDaily data
        raw_upload = self.s3_manager.upload_raw(
            hotel_code=context.hotel_code,
            data_type="reservations",
            data=chunk_records,
            custom_suffix=chunk_timestamp,
        )
        upload_key = f"reservations_raw_{chunk_index}" if chunk_index else "reservations_raw"
        context.add_s3_upload(upload_key, raw_upload)

        # Transform StatDaily to Climber reservations
        self.logger.info(
            f"Converting StatDaily to reservations for {chunk_label}",
            hotel_code=context.hotel_code,
        )

        reservation_collection = StatDailyToReservationTransformer.transform_batch(
            chunk_records,
            hotel_code=context.hotel_code,
            hotel_local_time=hotel_local_time,
            config_response=context.config_response,
            is_first_import=context.is_first_import,
        )

        # Upload processed reservations
        processed_upload = self.s3_manager.upload_processed(
            hotel_code=context.hotel_code,
            data_type="reservations",
            data=reservation_collection.reservations,
            custom_suffix=chunk_timestamp,
        )
        upload_key = f"reservations_processed_{chunk_index}" if chunk_index else "reservations_processed"
        context.add_s3_upload(upload_key, processed_upload)

        # Register with ESB
        await self.esb_client.register_file(
            hotel_code=context.hotel_code,
            file_type="reservations",
            file_url=processed_upload["url"],
            file_key=processed_upload["key"],
            record_count=reservation_collection.total_count,
            is_first_import=context.is_first_import,
        )

        self.logger.info(
            f"Successfully processed {chunk_label}",
            hotel_code=context.hotel_code,
            stat_daily_records=len(chunk_records),
            reservations_created=len(reservation_collection.reservations),
        )

        # Return stats for this chunk
        return {
            "raw_count": len(chunk_records),
            "reservation_count": len(reservation_collection.reservations),
            "chunk_records": chunk_records,  # Keep for context (last chunk only)
            "reservation_collection": reservation_collection,  # Keep for context (last chunk only)
        }

    async def execute(self, context: PipelineContext) -> bool:
        """Process StatDaily data and convert to reservations.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Use calculated date range from ESB parameters if available
            # Otherwise fall back to configuration-based calculation
            if context.calculated_stat_daily_start and context.calculated_stat_daily_end:
                start_date = datetime.fromisoformat(context.calculated_stat_daily_start).date()
                end_date = datetime.fromisoformat(context.calculated_stat_daily_end).date()

                self.logger.info(
                    "Using ESB-calculated StatDaily date range",
                    hotel_code=context.hotel_code,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    is_first_import=(context.last_import_date is None),
                )
            else:
                # Fallback: Calculate date range from configuration
                # Uses HOST_API_STAT_DAILY_DAYS_BACK_START and HOST_API_STAT_DAILY_DAYS_BACK_END
                # environment variables or defaults (95 days and 30 days)
                today = datetime.utcnow().date()
                start_date = today - timedelta(days=settings.host_pms.stat_daily_days_back_start)
                end_date = today - timedelta(days=settings.host_pms.stat_daily_days_back_end)

                self.logger.info(
                    "Using config-based StatDaily date range (fallback)",
                    hotel_code=context.hotel_code,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    days_back_start=settings.host_pms.stat_daily_days_back_start,
                    days_back_end=settings.host_pms.stat_daily_days_back_end,
                )

            # Extract hotel_local_time from config for record_date calculation
            hotel_local_time = None
            if context.config_response:
                try:
                    from src.models.host.config import HotelConfigResponse

                    # Convert dict to HotelConfigResponse if needed
                    if isinstance(context.config_response, dict):
                        config_model = HotelConfigResponse(**context.config_response)
                    else:
                        config_model = context.config_response

                    hotel_local_time = config_model.hotel_info.local_time
                    if hotel_local_time:
                        self.logger.info(
                            "Extracted hotel local time",
                            hotel_code=context.hotel_code,
                            local_time=str(hotel_local_time),
                        )
                except Exception as e:
                    self.logger.warning(
                        "Could not extract hotel local time",
                        hotel_code=context.hotel_code,
                        error=str(e),
                    )

            # Check if first import - use chunking to avoid memory issues
            if context.is_first_import:
                # Break into 4-month chunks for memory efficiency
                date_chunks = self._create_date_chunks(start_date, end_date)

                self.logger.info(
                    "First import detected - breaking into 4-month chunks",
                    hotel_code=context.hotel_code,
                    total_chunks=len(date_chunks),
                    date_range=f"{start_date.isoformat()} to {end_date.isoformat()}",
                )

                # Process each chunk separately
                total_raw_records = 0
                total_reservations = 0
                last_chunk_result = None

                for idx, (chunk_start, chunk_end) in enumerate(date_chunks, start=1):
                    chunk_result = await self._process_chunk(
                        context=context,
                        chunk_start=chunk_start,
                        chunk_end=chunk_end,
                        hotel_local_time=hotel_local_time,
                        chunk_index=idx,
                        total_chunks=len(date_chunks),
                    )

                    total_raw_records += chunk_result["raw_count"]
                    total_reservations += chunk_result["reservation_count"]
                    last_chunk_result = chunk_result  # Keep last chunk for context

                # Store last chunk data in context (memory-conscious)
                if last_chunk_result:
                    context.stat_daily_records = last_chunk_result.get("chunk_records", [])
                    context.reservations_collection = last_chunk_result.get("reservation_collection")

                # Store aggregated statistics
                context.stats["stat_daily"] = {
                    "raw_record_count": total_raw_records,
                    "reservations_created": total_reservations,
                    "chunks_processed": len(date_chunks),
                }

                self.logger.info(
                    "Successfully processed all chunks",
                    hotel_code=context.hotel_code,
                    total_chunks=len(date_chunks),
                    total_stat_daily_records=total_raw_records,
                    total_reservations=total_reservations,
                )

            else:
                # Regular import - process entire range at once
                chunk_result = await self._process_chunk(
                    context=context,
                    chunk_start=start_date,
                    chunk_end=end_date,
                    hotel_local_time=hotel_local_time,
                )

                # Store in context
                context.stat_daily_records = chunk_result.get("chunk_records", [])
                context.reservations_collection = chunk_result.get("reservation_collection")

                # Store statistics
                context.stats["stat_daily"] = {
                    "raw_record_count": chunk_result["raw_count"],
                    "reservations_created": chunk_result["reservation_count"],
                }

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process StatDaily data",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process StatDaily: {str(e)}")
            return False

    def is_required(self) -> bool:
        """StatDaily processing is optional.

        Returns:
            False
        """
        return False
