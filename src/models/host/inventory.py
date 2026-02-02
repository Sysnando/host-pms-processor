"""Pydantic models for Host PMS API inventory responses."""

from datetime import date
from typing import Any, Optional

from pydantic import BaseModel, Field


class DailyInventory(BaseModel):
    """Daily inventory information for a room."""

    date: date
    room_code: str = Field(alias="roomCode")
    inventory: int = 0  # Available rooms
    inventory_ooi: Optional[int] = Field(None, alias="inventoryOOI")  # Out of inventory
    inventory_ooo: Optional[int] = Field(None, alias="inventoryOOO")  # Out of order
    rate: Optional[float] = None  # Base rate for the day
    status: Optional[str] = None  # AVAILABLE, SOLD_OUT, CLOSED, etc.

    class Config:
        extra = "allow"
        populate_by_name = True


class RoomInventory(BaseModel):
    """Room-level inventory data."""

    room_code: str = Field(alias="roomCode")
    room_id: Optional[str] = Field(None, alias="roomId")
    room_name: Optional[str] = Field(None, alias="roomName")
    room_type_code: Optional[str] = Field(None, alias="roomTypeCode")
    daily_inventories: list[DailyInventory] = Field(
        default_factory=list, alias="dailyInventories"
    )

    class Config:
        extra = "allow"
        populate_by_name = True


class InventoryResponse(BaseModel):
    """Room inventory response from Host PMS API."""

    hotel_code: str = Field(alias="hotelCode")
    start_date: date = Field(alias="startDate")
    end_date: date = Field(alias="endDate")
    room_inventories: list[RoomInventory] = Field(
        default_factory=list, alias="roomInventories"
    )
    last_update_date: Optional[str] = Field(None, alias="lastUpdateDate")

    class Config:
        extra = "allow"
        populate_by_name = True
