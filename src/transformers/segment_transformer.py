"""Transformer for converting Host PMS segments to Climber standardized format."""

from typing import Any

from structlog import get_logger

from src.models.climber.segment import SegmentCollection, SegmentItem
from src.models.host.config import ConfigItem

logger = get_logger(__name__)

# Mapping of segment types to Climber segment categories
SEGMENT_TYPE_MAPPING = {
    "agency": "agencies",
    "channel": "channels",
    "company": "companies",
    "group": "groups",
    "package": "packages",
    "rate": "rates",
    "room": "rooms",
    "segment": "segments",
    "subsegment": "sub_segments",
    "sub_segment": "sub_segments",
    "unspecified": "segments",  # Default category for unknown types
}

# Default category for unmapped segment types
DEFAULT_SEGMENT_CATEGORY = "segments"


class SegmentTransformer:
    """Transforms Host PMS API segments to Climber standardized format."""

    @staticmethod
    def _get_segment_category(segment_type: str) -> str:
        """Get the Climber segment category for a Host PMS segment type.

        Args:
            segment_type: Segment type from Host PMS API

        Returns:
            Climber segment category name
        """
        normalized_type = segment_type.lower().strip() if segment_type else ""
        return SEGMENT_TYPE_MAPPING.get(normalized_type, DEFAULT_SEGMENT_CATEGORY)

    @staticmethod
    def transform(
        host_segments: list[dict[str, Any] | ConfigItem],
    ) -> SegmentCollection:
        """Transform Host PMS segments to Climber format.

        Unmapped segment types are assigned to 'UNASSIGNED' category as per requirements.

        Args:
            host_segments: List of segments from Host PMS API

        Returns:
            SegmentCollection in Climber standardized format
        """
        logger.info(
            "Transforming segments",
            segment_count=len(host_segments),
        )

        # Initialize collection with empty lists
        segment_collection = SegmentCollection()

        # Process each segment
        unmapped_count = 0
        for segment_data in host_segments:
            try:
                # Convert dict to ConfigItem model if needed
                if isinstance(segment_data, dict):
                    segment = ConfigItem(**segment_data)
                else:
                    segment = segment_data

                # Create SegmentItem
                segment_item = SegmentItem(
                    code=segment.code,
                    name=segment.name,
                    description=segment.description,
                    type=segment.type,
                )

                # Get category and add to appropriate list
                category = SegmentTransformer._get_segment_category(segment.type)

                # Get the attribute (e.g., "agencies", "channels", etc.)
                if hasattr(segment_collection, category):
                    getattr(segment_collection, category).append(segment_item)
                else:
                    # Fallback to segments if category not found
                    segment_collection.segments.append(segment_item)
                    unmapped_count += 1
                    logger.warning(
                        "Segment assigned to default category",
                        segment_code=segment.code,
                        segment_type=segment.type,
                        category="segments",
                    )

            except Exception as e:
                logger.warning(
                    "Failed to transform segment",
                    segment_data=str(segment_data)[:100],  # Truncate for logging
                    error=str(e),
                )
                # Continue with other segments even if one fails
                continue

        logger.info(
            "Successfully transformed segments",
            total_segments=len(host_segments),
            agencies=len(segment_collection.agencies),
            channels=len(segment_collection.channels),
            companies=len(segment_collection.companies),
            groups=len(segment_collection.groups),
            packages=len(segment_collection.packages),
            rates=len(segment_collection.rates),
            rooms=len(segment_collection.rooms),
            segments=len(segment_collection.segments),
            sub_segments=len(segment_collection.sub_segments),
            unmapped_count=unmapped_count,
        )

        return segment_collection

    @staticmethod
    def merge_segment_collections(
        collections: list[SegmentCollection],
    ) -> SegmentCollection:
        """Merge multiple segment collections into one.

        Args:
            collections: List of SegmentCollection objects

        Returns:
            Merged SegmentCollection
        """
        logger.info(
            "Merging segment collections",
            collection_count=len(collections),
        )

        merged = SegmentCollection()

        for collection in collections:
            merged.agencies.extend(collection.agencies)
            merged.channels.extend(collection.channels)
            merged.companies.extend(collection.companies)
            merged.groups.extend(collection.groups)
            merged.packages.extend(collection.packages)
            merged.rates.extend(collection.rates)
            merged.rooms.extend(collection.rooms)
            merged.segments.extend(collection.segments)
            merged.sub_segments.extend(collection.sub_segments)

        logger.info(
            "Successfully merged segment collections",
            total_agencies=len(merged.agencies),
            total_channels=len(merged.channels),
            total_companies=len(merged.companies),
            total_groups=len(merged.groups),
            total_packages=len(merged.packages),
            total_rates=len(merged.rates),
            total_rooms=len(merged.rooms),
            total_segments=len(merged.segments),
            total_sub_segments=len(merged.sub_segments),
        )

        return merged
