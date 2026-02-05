"""Step to process StatDaily data and update reservation invoices."""

from datetime import datetime, timedelta

from src.aws import S3Manager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.services.pipeline import PipelineContext, PipelineStep
from src.transformers import StatDailyTransformer


class ProcessStatDailyStep(PipelineStep):
    """Fetch StatDaily data and update reservation invoices."""

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
        """Process StatDaily data.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        try:
            # Calculate date range: 30 days ago to yesterday
            today = datetime.utcnow().date()
            yesterday = today - timedelta(days=1)
            start_date = today - timedelta(days=30)

            self.logger.info(
                "Fetching StatDaily date range",
                hotel_code=context.hotel_code,
                start_date=start_date.isoformat(),
                end_date=yesterday.isoformat(),
            )

            # Fetch StatDaily for each date
            all_stat_daily_records = []
            current_date = start_date

            while current_date <= yesterday:
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

                context.stat_daily_records = all_stat_daily_records

                # Update reservations with StatDaily invoice data
                if (
                    context.reservations_collection
                    and len(context.reservations_collection.reservations) > 0
                ):
                    updated_collection, stats = StatDailyTransformer.process_stat_daily_for_reservations(
                        all_stat_daily_records, context.reservations_collection
                    )

                    # Re-upload updated reservations to S3
                    updated_upload = self.s3_manager.upload_processed(
                        hotel_code=context.hotel_code,
                        data_type="reservations",
                        data=updated_collection,
                    )
                    context.add_s3_upload("reservations_processed", updated_upload)

                    # Update ESB registration with new file
                    await self.esb_client.register_file(
                        hotel_code=context.hotel_code,
                        file_type="reservation",
                        file_url=updated_upload["url"],
                        file_key=updated_upload["key"],
                        record_count=updated_collection.total_count,
                    )

                    # Update SQS message
                    for msg in context.sqs_messages:
                        if msg["file_type"] == "reservations":
                            msg["file_key"] = updated_upload["key"]

                    # Update context
                    context.reservations_collection = updated_collection

                    # Store statistics
                    context.stats["stat_daily"] = {
                        "record_count": len(all_stat_daily_records),
                        "updated_reservations": stats["updated_reservations"],
                        "match_rate": stats["match_rate"],
                    }

                    self.logger.info(
                        "Updated reservations with StatDaily invoice data",
                        hotel_code=context.hotel_code,
                        updated_count=stats["updated_reservations"],
                        match_rate=stats["match_rate"],
                    )

            return True

        except Exception as e:
            self.logger.warning(
                "Failed to process StatDaily data",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            # StatDaily is optional, so don't add to errors
            return True  # Return True to continue pipeline

    def is_required(self) -> bool:
        """StatDaily processing is optional.

        Returns:
            False
        """
        return False
