"""Pydantic models for Climber standardized hotel config format."""

from typing import Optional

from pydantic import BaseModel, Field, field_validator

from src.models._normalizers import _extract_code_str


class RoomDefinition(BaseModel):
    """Room definition in Climber format."""

    code: str = Field(description="Room code/type")
    name: str = Field(description="Room name")
    capacity: int = Field(default=0, description="Room capacity in guests")
    category: Optional[str] = Field(None, description="Room category")

    @field_validator("code", "name", mode="before")
    @classmethod
    def _normalize_code(cls, value):
        return _extract_code_str(value)

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
