"""Step to process segments."""

import asyncio

from src.aws import S3Manager
from src.clients import ClimberESBClient
from src.services.pipeline import PipelineContext, PipelineStep


class ProcessSegmentsStep(PipelineStep):
    """Upload and register segments extracted from config."""

    def __init__(
        self,
        esb_client: ClimberESBClient,
        s3_manager: S3Manager,
    ):
        """Initialize the step.

        Args:
            esb_client: Climber ESB API client
            s3_manager: S3 manager for uploads
        """
        super().__init__("ProcessSegments")
        self.esb_client = esb_client
        self.s3_manager = s3_manager

    async def execute(self, context: PipelineContext) -> bool:
        """Process segments.

        Args:
            context: Pipeline context

        Returns:
            True if successful, False otherwise
        """
        # Check if segments collection is available
        if context.segments_collection is None:
            self.logger.warning(
                "No segments collection available, skipping",
                hotel_code=context.hotel_code,
            )
            return False

        try:
            # Upload raw segments to S3 (same as processed, as segments are derived from config)
            raw_upload = await asyncio.to_thread(
                self.s3_manager.upload_raw,
                hotel_code=context.hotel_code,
                data_type="segments",
                data=context.segments_collection,
            )
            context.add_s3_upload("segments_raw", raw_upload)

            # Upload processed segments to S3
            processed_upload = await asyncio.to_thread(
                self.s3_manager.upload_processed,
                hotel_code=context.hotel_code,
                data_type="segments",
                data=context.segments_collection,
            )
            context.add_s3_upload("segments_processed", processed_upload)

            # Calculate total segments
            total_segments = (
                len(context.segments_collection.agencies)
                + len(context.segments_collection.channels)
                + len(context.segments_collection.companies)
                + len(context.segments_collection.groups)
                + len(context.segments_collection.packages)
                + len(context.segments_collection.rates)
                + len(context.segments_collection.rooms)
                + len(context.segments_collection.segments)
                + len(context.segments_collection.sub_segments)
            )

            # Register with ESB
            await self.esb_client.register_file(
                hotel_code=context.hotel_code,
                file_type="segments",
                file_url=processed_upload["url"],
                file_key=processed_upload["key"],
                record_count=total_segments,
                is_first_import=context.is_first_import,
                hotel_local_time=context.hotel_local_time,
            )

            # Store statistics
            context.stats["segments"] = {
                "total_segments": total_segments,
                "agencies": len(context.segments_collection.agencies),
                "channels": len(context.segments_collection.channels),
                "companies": len(context.segments_collection.companies),
                "groups": len(context.segments_collection.groups),
                "packages": len(context.segments_collection.packages),
                "rates": len(context.segments_collection.rates),
                "rooms": len(context.segments_collection.rooms),
                "segments": len(context.segments_collection.segments),
                "sub_segments": len(context.segments_collection.sub_segments),
            }

            self.logger.info(
                "Processed segments successfully",
                hotel_code=context.hotel_code,
                total_segments=total_segments,
            )

            return True

        except Exception as e:
            self.logger.error(
                "Failed to process segments",
                hotel_code=context.hotel_code,
                error=str(e),
            )
            context.add_error(self.name, f"Failed to process segments: {str(e)}")
            return False

    def is_required(self) -> bool:
        """Segments processing is optional.

        Returns:
            False
        """
        return False
