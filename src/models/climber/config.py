"""Pydantic models for Climber standardized hotel config format."""

from typing import Optional

from pydantic import BaseModel, Field


class RoomDefinition(BaseModel):
    """Room definition in Climber format."""

    code: str = Field(description="Room code/type")
    name: str = Field(description="Room name")
    capacity: int = Field(default=0, description="Room capacity in guests")
    category: Optional[str] = Field(None, description="Room category")

    class Config:
        extra = "allow"


class HotelConfigData(BaseModel):
    """Hotel configuration in Climber standardized format."""

    hotel_code: str = Field(alias="hotelCode")
    hotel_name: str = Field(alias="hotelName")
    rooms: list[RoomDefinition] = Field(default_factory=list)
    room_count: int = Field(default=0, alias="roomCount")

    class Config:
        populate_by_name = True
        extra = "allow"
