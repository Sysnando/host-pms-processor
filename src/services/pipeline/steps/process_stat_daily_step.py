"""Step to process StatDaily data and convert it to reservations.

This step is the primary source for reservation data. It fetches StatDaily records
from the Host PMS API and transforms them into Climber reservations using the
StatDailyToReservationTransformer.
"""

from datetime import datetime, timedelta

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
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

    async def execute(self, context: PipelineContext) -> bool:
        """Process StatDaily data and convert to reservations.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Calculate date range: 95 days ago to 30 days ago
            # This matches the logic from fetch_and_transform_local.py
            today = datetime.utcnow().date()
            end_date = today - timedelta(days=30)
            start_date = today - timedelta(days=95)

            self.logger.info(
                "Fetching StatDaily date range",
                hotel_code=context.hotel_code,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
            )

            # Fetch StatDaily for each date in range
            all_stat_daily_records = []
            current_date = start_date

            while current_date <= end_date:
                try:
                    date_str = current_date.isoformat()
                    stat_daily_response = await self.host_api_client.get_stat_daily(
                        hotel_date_filter=date_str
                    )

                    if isinstance(stat_daily_response, list):
                        all_stat_daily_records.extend(stat_daily_response)

                except Exception as e:
                    self.logger.warning(
                        "Failed to fetch StatDaily for date",
                        hotel_code=context.hotel_code,
                        date=current_date.isoformat(),
                        error=str(e),
                    )
                    # Continue with other dates even if one fails

                current_date += timedelta(days=1)

            self.logger.info(
                "Fetched StatDaily data",
                hotel_code=context.hotel_code,
                total_records=len(all_stat_daily_records),
            )

            # Upload raw StatDaily data
            if all_stat_daily_records:
                raw_upload = self.s3_manager.upload_raw(
                    hotel_code=context.hotel_code,
                    data_type="stat-daily",
                    data=all_stat_daily_records,
                )
                context.add_s3_upload("stat_daily_raw", raw_upload)

                # Store raw records in context
                context.stat_daily_records = all_stat_daily_records

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

                # Convert StatDaily to Climber reservations
                self.logger.info(
                    "Converting StatDaily to reservations",
                    hotel_code=context.hotel_code,
                )

                reservation_collection = StatDailyToReservationTransformer.transform_batch(
                    all_stat_daily_records,
                    hotel_code=context.hotel_code,
                    hotel_local_time=hotel_local_time,
                )

                # Upload processed reservations
                processed_upload = self.s3_manager.upload_processed(
                    hotel_code=context.hotel_code,
                    data_type="reservations",
                    data=reservation_collection,
                )
                context.add_s3_upload("reservations_processed", processed_upload)

                # Register with ESB
                await self.esb_client.register_file(
                    hotel_code=context.hotel_code,
                    file_type="reservation",
                    file_url=processed_upload["url"],
                    file_key=processed_upload["key"],
                    record_count=reservation_collection.total_count,
                )

                # Add SQS message
                context.add_sqs_message("reservations", processed_upload["key"])

                # Update context
                context.reservations_collection = reservation_collection

                # Store statistics
                context.stats["stat_daily"] = {
                    "raw_record_count": len(all_stat_daily_records),
                    "reservations_created": len(reservation_collection.reservations),
                }

                self.logger.info(
                    "Successfully converted StatDaily to reservations",
                    hotel_code=context.hotel_code,
                    stat_daily_records=len(all_stat_daily_records),
                    reservations_created=len(reservation_collection.reservations),
                )

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
