"""Main orchestrator service for the Host PMS connector ETL pipeline."""

from datetime import datetime
from typing import Any

from structlog import get_logger

from src.aws import S3Manager, SQSManager
from src.clients import ClimberESBClient, HostPMSAPIClient
from src.transformers import (
    ConfigTransformer,
    ReservationTransformer,
    StatDailyTransformer,
)

logger = get_logger(__name__)


class OrchestrationError(Exception):
    """Raised when orchestration fails."""

    pass


class HostPMSConnectorOrchestrator:
    """Main orchestrator for the Host PMS to Climber ETL pipeline."""

    def __init__(self):
        """Initialize the orchestrator with all required services."""
        self.esb_client = ClimberESBClient()
        self.host_api_client = HostPMSAPIClient()
        self.s3_manager = S3Manager()
        self.sqs_manager = SQSManager()

    async def process_hotel(
        self,
        hotel_code: str,
    ) -> dict[str, Any]:
        """Process a single hotel through the ETL pipeline.

        Args:
            hotel_code: Hotel code to process

        Returns:
            Dictionary with processing results and statistics

        Raises:
            OrchestrationError: If critical processing fails
        """
        logger.info(
            "Starting hotel processing",
            hotel_code=hotel_code,
        )

        results = {
            "hotel_code": hotel_code,
            "success": False,
            "config": None,
            "segments": None,
            "reservations": None,
            "inventory": None,
            "revenue": None,
            "errors": [],
            "sqs_messages": [],
        }

        try:
            # Step 1: Fetch import parameters from ESB
            logger.info("Fetching import parameters", hotel_code=hotel_code)
            try:
                parameters = await self.esb_client.get_hotel_parameters(hotel_code)
                last_import_date = parameters.get("lastImportDate")
                logger.info(
                    "Successfully fetched import parameters",
                    hotel_code=hotel_code,
                    last_import_date=last_import_date,
                )
            except Exception as e:
                logger.error(
                    "Failed to fetch import parameters",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to fetch import parameters: {str(e)}")
                raise OrchestrationError(
                    f"Cannot proceed without import parameters for {hotel_code}"
                ) from e

            # Step 2: Extract hotel config (includes segments and other config items)
            logger.info("Extracting hotel config", hotel_code=hotel_code)
            try:
                config_response = await self.host_api_client.get_hotel_config(
                    hotel_code
                )

                # Upload raw config
                raw_upload = self.s3_manager.upload_raw(
                    hotel_code=hotel_code,
                    data_type="hotel-configs",
                    data=config_response,
                )

                # Transform config (returns config + segments)
                config_data, segments_collection = ConfigTransformer.transform(config_response)

                # Upload processed hotel config
                processed_config_upload = self.s3_manager.upload_processed(
                    hotel_code=hotel_code,
                    data_type="hotel-configs",
                    data=config_data,
                )

                # Register hotel config file with ESB
                await self.esb_client.register_file(
                    hotel_code=hotel_code,
                    file_type="config",
                    file_url=processed_config_upload["url"],
                    file_key=processed_config_upload["key"],
                    record_count=config_data.room_count,
                )

                results["config"] = {
                    "raw_key": raw_upload["key"],
                    "processed_key": processed_config_upload["key"],
                    "room_count": config_data.room_count,
                }

                # Store SQS message for config
                results["sqs_messages"].append(
                    {
                        "hotel_code": hotel_code,
                        "file_type": "config",
                        "file_key": processed_config_upload["key"],
                    }
                )

                logger.info(
                    "Successfully processed hotel config",
                    hotel_code=hotel_code,
                    room_count=config_data.room_count,
                )

            except Exception as e:
                logger.error(
                    "Failed to extract/process hotel config",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to process config: {str(e)}")
                # Continue to try processing other data even if config fails
                segments_collection = None

            # Step 3: Extract and upload room inventory (from config response)
            logger.info("Extracting room inventory from config", hotel_code=hotel_code)
            try:
                # Extract room inventory from config
                room_inventory = ConfigTransformer.get_room_inventory(config_response)

                # Upload processed room inventory
                processed_inventory_upload = self.s3_manager.upload_processed(
                    hotel_code=hotel_code,
                    data_type="inventory",
                    data=room_inventory,
                )

                # Register inventory file with ESB
                await self.esb_client.register_file(
                    hotel_code=hotel_code,
                    file_type="inventory",
                    file_url=processed_inventory_upload["url"],
                    file_key=processed_inventory_upload["key"],
                    record_count=len(room_inventory.room_inventory),
                )

                results["inventory"] = {
                    "processed_key": processed_inventory_upload["key"],
                    "room_count": len(room_inventory.room_inventory),
                }

                # Store SQS message for inventory
                results["sqs_messages"].append(
                    {
                        "hotel_code": hotel_code,
                        "file_type": "inventory",
                        "file_key": processed_inventory_upload["key"],
                    }
                )

                logger.info(
                    "Successfully extracted room inventory",
                    hotel_code=hotel_code,
                    room_count=len(room_inventory.room_inventory),
                )

            except Exception as e:
                logger.error(
                    "Failed to extract room inventory",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to extract room inventory: {str(e)}")

            # Step 4: Process segments (extracted from config response)
            if segments_collection is not None:
                logger.info("Processing segments from config", hotel_code=hotel_code)
                try:
                    # Upload processed segments
                    processed_segments_upload = self.s3_manager.upload_processed(
                        hotel_code=hotel_code,
                        data_type="segments",
                        data=segments_collection,
                    )

                    # Register segments file with ESB
                    total_segments = (
                        len(segments_collection.agencies)
                        + len(segments_collection.channels)
                        + len(segments_collection.companies)
                        + len(segments_collection.groups)
                        + len(segments_collection.packages)
                        + len(segments_collection.rates)
                        + len(segments_collection.rooms)
                        + len(segments_collection.segments)
                        + len(segments_collection.sub_segments)
                    )

                    await self.esb_client.register_file(
                        hotel_code=hotel_code,
                        file_type="segments",
                        file_url=processed_segments_upload["url"],
                        file_key=processed_segments_upload["key"],
                        record_count=total_segments,
                    )

                    results["segments"] = {
                        "processed_key": processed_segments_upload["key"],
                        "total_segments": total_segments,
                        "breakdown": {
                            "agencies": len(segments_collection.agencies),
                            "channels": len(segments_collection.channels),
                            "companies": len(segments_collection.companies),
                            "groups": len(segments_collection.groups),
                            "packages": len(segments_collection.packages),
                            "rates": len(segments_collection.rates),
                            "rooms": len(segments_collection.rooms),
                            "segments": len(segments_collection.segments),
                            "sub_segments": len(segments_collection.sub_segments),
                        },
                    }

                    # Store SQS message for segments
                    results["sqs_messages"].append(
                        {
                            "hotel_code": hotel_code,
                            "file_type": "segments",
                            "file_key": processed_segments_upload["key"],
                        }
                    )

                    logger.info(
                        "Successfully processed segments",
                        hotel_code=hotel_code,
                        total_segments=total_segments,
                    )

                except Exception as e:
                    logger.error(
                        "Failed to process segments",
                        hotel_code=hotel_code,
                        error=str(e),
                    )
                    results["errors"].append(f"Failed to process segments: {str(e)}")

            # Step 5: Extract and process reservations
            logger.info("Extracting reservations", hotel_code=hotel_code)
            reservations_collection = None  # Initialize to None
            try:
                reservations_response = await self.host_api_client.get_reservations(
                    hotel_code=hotel_code,
                    update_from=last_import_date,
                )

                # Upload raw reservations
                raw_upload = self.s3_manager.upload_raw(
                    hotel_code=hotel_code,
                    data_type="reservations",
                    data=reservations_response,
                )

                # Transform reservations
                if isinstance(reservations_response, dict):
                    reservations_data = reservations_response.get("Reservations", [])
                else:
                    reservations_data = reservations_response.reservations

                reservations_collection, _, _, _ = ReservationTransformer.transform_batch(
                    reservations_data,
                    hotel_code=hotel_code,
                )

                # Upload processed reservations
                processed_upload = self.s3_manager.upload_processed(
                    hotel_code=hotel_code,
                    data_type="reservations",
                    data=reservations_collection,
                )

                # Register file with ESB
                await self.esb_client.register_file(
                    hotel_code=hotel_code,
                    file_type="reservation",
                    file_url=processed_upload["url"],
                    file_key=processed_upload["key"],
                    record_count=reservations_collection.total_count,
                )

                results["reservations"] = {
                    "raw_key": raw_upload["key"],
                    "processed_key": processed_upload["key"],
                    "record_count": reservations_collection.total_count,
                }

                # Store message for SQS
                results["sqs_messages"].append(
                    {
                        "hotel_code": hotel_code,
                        "file_type": "reservations",
                        "file_key": processed_upload["key"],
                    }
                )

                logger.info(
                    "Successfully processed reservations",
                    hotel_code=hotel_code,
                    record_count=reservations_collection.total_count,
                )

            except Exception as e:
                logger.error(
                    "Failed to extract/process reservations",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to process reservations: {str(e)}")

            # Step 5.5: Fetch StatDaily and update reservation invoices
            logger.info("Fetching StatDaily data for invoice updates", hotel_code=hotel_code)
            try:
                # Calculate date range: 30 days ago to yesterday
                from datetime import datetime, timedelta
                today = datetime.utcnow().date()
                yesterday = today - timedelta(days=1)
                start_date = today - timedelta(days=30)

                # Fetch StatDaily for each date
                all_stat_daily_records = []
                current_date = start_date

                logger.info(
                    "Fetching StatDaily date range",
                    hotel_code=hotel_code,
                    start_date=start_date.isoformat(),
                    end_date=yesterday.isoformat(),
                )

                while current_date <= yesterday:
                    try:
                        date_str = current_date.isoformat()
                        stat_daily_response = await self.host_api_client.get_stat_daily(
                            hotel_date_filter=date_str
                        )

                        if isinstance(stat_daily_response, list):
                            all_stat_daily_records.extend(stat_daily_response)

                    except Exception as e:
                        logger.warning(
                            "Failed to fetch StatDaily for date",
                            hotel_code=hotel_code,
                            date=current_date.isoformat(),
                            error=str(e),
                        )
                        # Continue with other dates even if one fails

                    current_date += timedelta(days=1)

                logger.info(
                    "Successfully fetched StatDaily data",
                    hotel_code=hotel_code,
                    total_records=len(all_stat_daily_records),
                )

                # Upload raw StatDaily data
                if all_stat_daily_records:
                    raw_stat_daily_upload = self.s3_manager.upload_raw(
                        hotel_code=hotel_code,
                        data_type="stat-daily",
                        data=all_stat_daily_records,
                    )

                    # Update reservations with StatDaily invoice data
                    if reservations_collection and len(reservations_collection.reservations) > 0:
                        updated_collection, stats = StatDailyTransformer.process_stat_daily_for_reservations(
                            all_stat_daily_records,
                            reservations_collection
                        )

                        # Re-upload updated reservations to S3
                        updated_processed_upload = self.s3_manager.upload_processed(
                            hotel_code=hotel_code,
                            data_type="reservations",
                            data=updated_collection,
                        )

                        # Update ESB registration with new file
                        await self.esb_client.register_file(
                            hotel_code=hotel_code,
                            file_type="reservation",
                            file_url=updated_processed_upload["url"],
                            file_key=updated_processed_upload["key"],
                            record_count=updated_collection.total_count,
                        )

                        # Update results
                        results["reservations"]["processed_key"] = updated_processed_upload["key"]
                        results["reservations"]["invoice_updates"] = stats["updated_reservations"]

                        # Update SQS message
                        for msg in results["sqs_messages"]:
                            if msg["file_type"] == "reservations":
                                msg["file_key"] = updated_processed_upload["key"]

                        logger.info(
                            "Successfully updated reservations with StatDaily invoice data",
                            hotel_code=hotel_code,
                            updated_count=stats["updated_reservations"],
                            match_rate=stats["match_rate"],
                        )

                    results["stat_daily"] = {
                        "raw_key": raw_stat_daily_upload["key"],
                        "record_count": len(all_stat_daily_records),
                    }

            except Exception as e:
                logger.warning(
                    "Failed to process StatDaily data",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                # Don't add to errors list - StatDaily is optional
                # Continue with normal flow

            # Step 6: Update import date in ESB
            logger.info("Updating import date", hotel_code=hotel_code)
            try:
                await self.esb_client.update_import_date(
                    hotel_code=hotel_code,
                    last_import_date=datetime.utcnow().isoformat(),
                )
                logger.info(
                    "Successfully updated import date",
                    hotel_code=hotel_code,
                )
            except Exception as e:
                logger.warning(
                    "Failed to update import date",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to update import date: {str(e)}")

            # Step 7: Send SQS messages to trigger PMS Processor
            logger.info(
                "Sending SQS messages",
                hotel_code=hotel_code,
                message_count=len(results["sqs_messages"]),
            )
            try:
                for message in results["sqs_messages"]:
                    sqs_result = self.sqs_manager.send_message(
                        hotel_code=message["hotel_code"],
                        file_type=message["file_type"],
                        file_key=message["file_key"],
                    )
                    message["sqs_message_id"] = sqs_result["message_id"]

                logger.info(
                    "Successfully sent SQS messages",
                    hotel_code=hotel_code,
                    message_count=len(results["sqs_messages"]),
                )
            except Exception as e:
                logger.error(
                    "Failed to send SQS messages",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                results["errors"].append(f"Failed to send SQS messages: {str(e)}")

            # Mark as success
            results["success"] = len(results["errors"]) == 0

            logger.info(
                "Hotel processing complete",
                hotel_code=hotel_code,
                success=results["success"],
                error_count=len(results["errors"]),
            )

            return results

        except Exception as e:
            logger.error(
                "Unexpected error processing hotel",
                hotel_code=hotel_code,
                error=str(e),
            )
            results["errors"].append(f"Unexpected error: {str(e)}")
            return results

    async def process_all_hotels(self) -> dict[str, Any]:
        """Process all configured hotels through the ETL pipeline.

        Returns:
            Dictionary with aggregated results for all hotels
        """
        logger.info("Starting batch processing of all hotels")

        all_results = {
            "total_hotels": 0,
            "successful_hotels": 0,
            "failed_hotels": 0,
            "hotels": [],
            "start_time": datetime.utcnow().isoformat(),
        }

        try:
            # Step 1: Get list of configured hotels from ESB
            logger.info("Fetching hotel list from ESB")
            hotels = await self.esb_client.get_hotels()
            all_results["total_hotels"] = len(hotels)

            logger.info("Successfully fetched hotels", hotel_count=len(hotels))

            # Step 2: Process each hotel
            for hotel in hotels:
                hotel_code = hotel.get("code") or hotel.get("hotelCode")

                if not hotel_code:
                    logger.warning("Skipping hotel with no code", hotel=hotel)
                    continue

                try:
                    result = await self.process_hotel(hotel_code)
                    all_results["hotels"].append(result)

                    if result["success"]:
                        all_results["successful_hotels"] += 1
                    else:
                        all_results["failed_hotels"] += 1

                except Exception as e:
                    logger.error(
                        "Failed to process hotel",
                        hotel_code=hotel_code,
                        error=str(e),
                    )
                    all_results["hotels"].append(
                        {
                            "hotel_code": hotel_code,
                            "success": False,
                            "errors": [str(e)],
                        }
                    )
                    all_results["failed_hotels"] += 1

            all_results["end_time"] = datetime.utcnow().isoformat()

            logger.info(
                "Batch processing complete",
                total_hotels=all_results["total_hotels"],
                successful=all_results["successful_hotels"],
                failed=all_results["failed_hotels"],
            )

            return all_results

        except Exception as e:
            logger.error(
                "Batch processing failed",
                error=str(e),
            )
            all_results["error"] = str(e)
            all_results["end_time"] = datetime.utcnow().isoformat()
            return all_results
