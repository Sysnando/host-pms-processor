"""Transformer to convert Host PMS InventoryGrid API response to Climber format."""

from datetime import datetime, timedelta
from typing import Any

from structlog import get_logger

from src.models.climber.inventory import RoomInventoryData, RoomInventoryItem

logger = get_logger(__name__)


class InventoryGridTransformer:
    """Transform Host PMS InventoryGrid data to Climber hotel config format.

    Converts the detailed daily inventory data from the InventoryGrid API
    into the Climber roomInventory format with date ranges.
    """

    @staticmethod
    def transform(inventory_response: dict[str, Any]) -> RoomInventoryData:
        """Transform inventory grid response to Climber format.

        Converts Host PMS InventoryGrid response structure:
        {
            "roomInventories": [
                {
                    "roomCode": "D",
                    "dailyInventories": [
                        {
                            "date": "2026-03-04",
                            "inventory": 10,
                            "inventoryOOI": 0,
                            "inventoryOOO": 0
                        }
                    ]
                }
            ]
        }

        To Climber format:
        {
            "roomInventory": [
                {
                    "calendarDate": "[2026-03-04,2026-03-05)",
                    "inventory": 10,
                    "inventoryOOI": 0,
                    "inventoryOOO": 0,
                    "roomCode": "D"
                }
            ]
        }

        Args:
            inventory_response: Response from Host PMS InventoryGrid API

        Returns:
            RoomInventoryData in Climber format
        """
        room_inventories = inventory_response.get("roomInventories", [])

        if not room_inventories:
            logger.warning("No room inventories in response")
            return RoomInventoryData(room_inventory=[])

        inventory_items = []

        for room_inventory in room_inventories:
            room_code = room_inventory.get("roomCode")
            daily_inventories = room_inventory.get("dailyInventories", [])

            if not room_code:
                logger.warning("Room inventory missing roomCode, skipping")
                continue

            # Process each daily inventory
            for daily_inv in daily_inventories:
                date_str = daily_inv.get("date")

                if not date_str:
                    logger.warning("Daily inventory missing date, skipping", room_code=room_code)
                    continue

                try:
                    # Parse date
                    if isinstance(date_str, str):
                        date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    else:
                        date_obj = date_str

                    # Create date range for this single day: [date, date+1)
                    start_date = date_obj.date().isoformat()
                    end_date = (date_obj.date() + timedelta(days=1)).isoformat()
                    calendar_date = f"[{start_date},{end_date})"

                    # Create inventory item
                    inventory_item = RoomInventoryItem(
                        calendar_date=calendar_date,
                        inventory=daily_inv.get("inventory", 0),
                        inventory_ooi=daily_inv.get("inventoryOOI", 0),
                        inventory_ooo=daily_inv.get("inventoryOOO", 0),
                        room_code=room_code,
                    )

                    inventory_items.append(inventory_item)

                except Exception as e:
                    logger.warning(
                        "Failed to parse daily inventory",
                        room_code=room_code,
                        date=date_str,
                        error=str(e),
                    )
                    continue

        logger.info(
            "Transformed inventory grid to Climber format", total_items=len(inventory_items)
        )

        return RoomInventoryData(room_inventory=inventory_items)

    @staticmethod
    def transform_with_grouping(inventory_response: dict[str, Any]) -> RoomInventoryData:
        """Transform inventory grid with consecutive date grouping.

        Groups consecutive dates with the same inventory values into single
        date ranges for more compact representation.

        Example:
            If dates 2026-03-04, 2026-03-05, 2026-03-06 all have inventory=10,
            they will be combined into a single range: [2026-03-04,2026-03-07)

        Args:
            inventory_response: Response from Host PMS InventoryGrid API

        Returns:
            RoomInventoryData in Climber format with grouped date ranges
        """
        room_inventories = inventory_response.get("roomInventories", [])

        if not room_inventories:
            logger.warning("No room inventories in response")
            return RoomInventoryData(room_inventory=[])

        inventory_items = []

        for room_inventory in room_inventories:
            room_code = room_inventory.get("roomCode")
            daily_inventories = room_inventory.get("dailyInventories", [])

            if not room_code:
                logger.warning("Room inventory missing roomCode, skipping")
                continue

            # Sort daily inventories by date
            sorted_inventories = sorted(daily_inventories, key=lambda x: x.get("date", ""))

            # Group consecutive dates with same inventory values
            grouped = []
            current_group = None

            for daily_inv in sorted_inventories:
                date_str = daily_inv.get("date")

                if not date_str:
                    continue

                try:
                    # Parse date
                    if isinstance(date_str, str):
                        date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
                    else:
                        date_obj = date_str.date() if hasattr(date_str, "date") else date_str

                    inv_values = {
                        "inventory": daily_inv.get("inventory", 0),
                        "inventoryOOI": daily_inv.get("inventoryOOI", 0),
                        "inventoryOOO": daily_inv.get("inventoryOOO", 0),
                    }

                    # Check if this continues the current group
                    if current_group is None:
                        # Start new group
                        current_group = {
                            "start_date": date_obj,
                            "end_date": date_obj + timedelta(days=1),
                            "values": inv_values,
                        }
                    elif (
                        current_group["end_date"] == date_obj
                        and current_group["values"] == inv_values
                    ):
                        # Extend current group
                        current_group["end_date"] = date_obj + timedelta(days=1)
                    else:
                        # Save current group and start new one
                        grouped.append(current_group)
                        current_group = {
                            "start_date": date_obj,
                            "end_date": date_obj + timedelta(days=1),
                            "values": inv_values,
                        }

                except Exception as e:
                    logger.warning(
                        "Failed to parse daily inventory",
                        room_code=room_code,
                        date=date_str,
                        error=str(e),
                    )
                    continue

            # Add last group
            if current_group:
                grouped.append(current_group)

            # Create inventory items from groups
            for group in grouped:
                calendar_date = (
                    f"[{group['start_date'].isoformat()},{group['end_date'].isoformat()})"
                )

                inventory_item = RoomInventoryItem(
                    calendar_date=calendar_date,
                    inventory=group["values"]["inventory"],
                    inventory_ooi=group["values"]["inventoryOOI"],
                    inventory_ooo=group["values"]["inventoryOOO"],
                    room_code=room_code,
                )

                inventory_items.append(inventory_item)

        logger.info("Transformed inventory grid with grouping", total_items=len(inventory_items))

        return RoomInventoryData(room_inventory=inventory_items)
