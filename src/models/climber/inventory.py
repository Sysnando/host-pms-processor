"""Pydantic models for Climber standardized inventory format."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RoomInventoryItem(BaseModel):
    """Single room inventory entry for Climber format.

    Represents the inventory configuration for a specific room type.
    The calendarDate field defines the date range this inventory is valid for.
    """

    calendar_date: str = Field(
        ...,
        alias="calendarDate",
        description="Date range in ISO format, e.g., '[2021-02-02,)' for open-ended or '[2021-02-02,2021-02-03)' for specific range",
    )
    inventory: int = Field(
        default=0, description="Number of available rooms of this type"
    )
    inventory_ooi: int = Field(
        default=0,
        alias="inventoryOOI",
        description="Out of Inventory flag (0=FALSE, 1=TRUE)",
    )
    inventory_ooo: int = Field(
        default=0,
        alias="inventoryOOO",
        description="Out of Occupation flag (0=FALSE, 1=TRUE)",
    )
    room_code: str = Field(
        alias="roomCode", description="Room type/category code (same as in segments and reservations)"
    )

    class Config:
        populate_by_name = True
        extra = "allow"


class RoomInventoryData(BaseModel):
    """Room inventory collection in Climber format.

    Contains inventory configuration for all room types in the property.
    """

    room_inventory: list[RoomInventoryItem] = Field(
        default_factory=list,
        alias="roomInventory",
        description="List of room inventory items",
    )

    class Config:
        populate_by_name = True
        extra = "allow"

    def to_climber_dict(self) -> dict:
        """Convert to Climber API expected format with camelCase keys.

        Returns:
            Dictionary with camelCase keys for Climber API
        """
        return {
            "roomInventory": [
                item.model_dump(by_alias=True) for item in self.room_inventory
            ]
        }


class RoomInventoryDay(BaseModel):
    """Legacy: Single day's inventory for a room in Climber format."""

    calendar_date: str = Field(
        ..., description="Calendar date range in ISO format, e.g., '[2021-02-02,)' or '[2021-02-02,2021-02-03)'"
    )
    inventory: int = Field(default=0, description="Number of available rooms")
    inventory_ooi: int = Field(
        default=0, alias="inventoryOOI", description="Out of inventory"
    )
    inventory_ooo: int = Field(
        default=0, alias="inventoryOOO", description="Out of order"
    )
    room_code: str = Field(alias="roomCode", description="Room type/code identifier")
    rate: Optional[float] = None
    status: Optional[str] = None

    class Config:
        populate_by_name = True
        extra = "allow"
