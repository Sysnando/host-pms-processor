"""Transformer for converting Host PMS hotel config to Climber standardized format."""

from typing import Any

from structlog import get_logger

from src.models.climber.config import HotelConfigData, RoomDefinition
from src.models.climber.inventory import RoomInventoryData, RoomInventoryItem
from src.models.climber.segment import SegmentCollection, SegmentItem
from src.models.host.config import ConfigItem, HotelConfigResponse

logger = get_logger(__name__)


class ConfigTransformer:
    """Transforms Host PMS API config to Climber standardized format."""

    @staticmethod
    def _transform_config_item_to_segment(item: ConfigItem) -> SegmentItem:
        """Convert a ConfigItem to a SegmentItem matching Climber format.

        Maps Host PMS configuration to Climber segment with occupancy and revenue flags.

        Args:
            item: ConfigItem from Host PMS API

        Returns:
            SegmentItem in Climber format with enabledOtb, enabledRevenue, and position
        """
        # Active in Host PMS determines both OTB and revenue impact
        enabled = item.active

        return SegmentItem(
            code=item.code,
            name=item.description,
            enabled_otb=enabled,
            enabled_revenue=enabled,
            position=9999,  # Default position
        )

    @staticmethod
    def transform(
        host_config: dict[str, Any] | HotelConfigResponse,
    ) -> tuple[HotelConfigData, SegmentCollection]:
        """Transform Host PMS hotel config to Climber format.

        Returns both the hotel configuration and complete segment collection.

        Args:
            host_config: Hotel config from Host PMS API (dict or HotelConfigResponse model)

        Returns:
            Tuple of (HotelConfigData, SegmentCollection) in Climber standardized format

        Raises:
            ValueError: If required fields are missing
        """
        # Convert dict to HotelConfigResponse model if needed
        if isinstance(host_config, dict):
            try:
                host_config = HotelConfigResponse(**host_config)
            except Exception as e:
                logger.error(
                    "Failed to parse host config response",
                    error=str(e),
                )
                raise ValueError(
                    f"Invalid host config response format: {str(e)}"
                ) from e

        hotel_code = host_config.hotel_info.hotel_code
        hotel_name = host_config.hotel_info.hotel_name

        logger.info(
            "Transforming hotel config",
            hotel_code=hotel_code,
            config_item_count=len(host_config.config_info),
        )

        # Transform rooms (CATEGORY type)
        rooms = []
        for room_item in host_config.rooms:
            try:
                room_def = RoomDefinition(
                    code=room_item.code,
                    name=room_item.description,
                    capacity=room_item.inventory,
                    category=room_item.code,
                )
                rooms.append(room_def)
            except Exception as e:
                logger.warning(
                    "Failed to transform room",
                    hotel_code=hotel_code,
                    room_code=room_item.code,
                    error=str(e),
                )
                continue

        # Create Climber hotel config
        climber_config = HotelConfigData(
            hotel_code=hotel_code,
            hotel_name=hotel_name,
            rooms=rooms,
            room_count=len(rooms),
        )

        logger.info(
            "Successfully transformed hotel rooms",
            hotel_code=hotel_code,
            room_count=len(rooms),
        )

        # Transform all segments and related items into Climber format
        segment_collection = SegmentCollection()

        # Transform ROOM CATEGORIES to rooms (CATEGORY type)
        logger.info("Transforming room categories", hotel_code=hotel_code, room_count=len(host_config.rooms))
        for item in host_config.rooms:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                # Rooms also go in segment_collection.rooms
                segment_collection.rooms.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform room category",
                    hotel_code=hotel_code,
                    room_code=item.code,
                    error=str(e),
                )
                continue

        # Transform SEGMENT items (market segments)
        logger.info("Transforming segments", hotel_code=hotel_code, segment_count=len(host_config.segments))
        for item in host_config.segments:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                segment_collection.segments.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform segment",
                    hotel_code=hotel_code,
                    segment_code=item.code,
                    error=str(e),
                )
                continue

        # Transform SUB-SEGMENT items
        logger.info(
            "Transforming sub-segments", hotel_code=hotel_code, count=len(host_config.sub_segments)
        )
        for item in host_config.sub_segments:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                segment_collection.sub_segments.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform sub-segment",
                    hotel_code=hotel_code,
                    code=item.code,
                    error=str(e),
                )
                continue

        # Transform DIST CHANNEL items to channels
        logger.info(
            "Transforming distribution channels", hotel_code=hotel_code, count=len(host_config.channels)
        )
        for item in host_config.channels:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                segment_collection.channels.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform distribution channel",
                    hotel_code=hotel_code,
                    code=item.code,
                    error=str(e),
                )
                continue

        # Transform PACKAGE items
        logger.info("Transforming packages", hotel_code=hotel_code, count=len(host_config.packages))
        for item in host_config.packages:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                segment_collection.packages.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform package",
                    hotel_code=hotel_code,
                    code=item.code,
                    error=str(e),
                )
                continue

        # Transform PRICELIST items to rates
        logger.info("Transforming price lists", hotel_code=hotel_code, count=len(host_config.price_lists))
        for item in host_config.price_lists:
            try:
                segment_item = ConfigTransformer._transform_config_item_to_segment(item)
                segment_collection.rates.append(segment_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform price list",
                    hotel_code=hotel_code,
                    code=item.code,
                    error=str(e),
                )
                continue

        # NOTE: Host PMS doesn't have agencies or CROs in ConfigInfo
        # These would need to come from other sources or be left empty
        # cros field is initialized as empty list by default

        logger.info(
            "Successfully transformed all configuration items to Climber format",
            hotel_code=hotel_code,
            rooms=len(segment_collection.rooms),
            agencies=len(segment_collection.agencies),
            channels=len(segment_collection.channels),
            companies=len(segment_collection.companies),
            cros=len(segment_collection.cros),
            groups=len(segment_collection.groups),
            packages=len(segment_collection.packages),
            rates=len(segment_collection.rates),
            segments=len(segment_collection.segments),
            sub_segments=len(segment_collection.sub_segments),
        )

        return climber_config, segment_collection

    @staticmethod
    def get_reservation_statuses(host_config: HotelConfigResponse) -> dict[int, str]:
        """Extract reservation status mappings from config.

        Maps Host PMS ConfigId (integer) to status code strings (e.g., 10 -> "CI", 20 -> "CO").

        Args:
            host_config: HotelConfigResponse with reservation status information

        Returns:
            Dictionary mapping ConfigId integers to status code strings
        """
        from src.models.reservation_status import ReservationStatusMapper

        status_map = {}

        for item in host_config.reservation_statuses:
            # Map ConfigId (integer) to status code string (e.g., 10 -> "CI")
            status_map[item.config_id] = item.code

        logger.info(
            "Extracted reservation statuses",
            status_count=len(status_map),
            status_mappings=[(config_id, code) for config_id, code in status_map.items()],
        )

        return status_map

    @staticmethod
    def get_charges(host_config: HotelConfigResponse) -> list[SegmentItem]:
        """Extract charge definitions from config.

        Args:
            host_config: HotelConfigResponse with charge information

        Returns:
            List of SegmentItem representing charges
        """
        charges = []

        for item in host_config.charges:
            try:
                charge_item = SegmentItem(
                    code=item.code,
                    name=item.description,
                )
                charges.append(charge_item)
            except Exception as e:
                logger.warning(
                    "Failed to transform charge",
                    code=item.code,
                    error=str(e),
                )  # Note: hotel_code not available in this method
                continue

        logger.info(
            "Extracted charges",
            charge_count=len(charges),
        )

        return charges

    @staticmethod
    def get_room_inventory(host_config: dict[str, Any] | HotelConfigResponse) -> RoomInventoryData:
        """Extract room inventory from config.

        Maps CATEGORY items to RoomInventoryItem objects for Climber format.
        Uses the Inventory field from each CATEGORY to set room availability.
        Calendar date range is set to open-ended from today onwards: "[today,)"

        Args:
            host_config: Hotel config from Host PMS API (dict or HotelConfigResponse model)

        Returns:
            RoomInventoryData with all room inventory items
        """
        from datetime import datetime

        # Convert dict to HotelConfigResponse model if needed
        if isinstance(host_config, dict):
            try:
                host_config = HotelConfigResponse(**host_config)
            except Exception as e:
                logger.error(
                    "Failed to parse host config response",
                    error=str(e),
                )
                raise ValueError(
                    f"Invalid host config response format: {str(e)}"
                ) from e

        hotel_code = host_config.hotel_info.hotel_code

        logger.info(
            "Extracting room inventory from config",
            hotel_code=hotel_code,
            room_count=len(host_config.rooms),
        )

        room_inventory = []
        today = datetime.utcnow().date().isoformat()
        calendar_date_range = f"[{today},)"  # Open-ended range from today onwards

        for room_item in host_config.rooms:
            try:
                inventory_item = RoomInventoryItem(
                    calendar_date=calendar_date_range,
                    inventory=room_item.inventory,
                    inventory_ooi=0,  # Host PMS doesn't provide OOI flag
                    inventory_ooo=0,  # Host PMS doesn't provide OOO flag
                    room_code=room_item.code,
                )
                room_inventory.append(inventory_item)

                logger.debug(
                    "Transformed room inventory item",
                    hotel_code=hotel_code,
                    room_code=room_item.code,
                    inventory=room_item.inventory,
                )

            except Exception as e:
                logger.warning(
                    "Failed to transform room inventory",
                    hotel_code=hotel_code,
                    room_code=room_item.code,
                    error=str(e),
                )
                continue

        inventory_data = RoomInventoryData(room_inventory=room_inventory)

        logger.info(
            "Successfully extracted room inventory",
            hotel_code=hotel_code,
            room_count=len(room_inventory),
            calendar_date_range=calendar_date_range,
        )

        return inventory_data
