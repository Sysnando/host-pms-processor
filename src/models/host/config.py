"""Pydantic models for Host PMS API configuration responses."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class ConfigItem(BaseModel):
    """Generic configuration item from Host PMS API.

    Can represent rooms (CATEGORY), segments, channels, packages, pricelists, charges, etc.
    """

    config_type: str = Field(alias="ConfigType")
    config_id: int = Field(alias="ConfigId")
    code: str = Field(alias="Code")
    description: str = Field(alias="Description")
    inventory: Optional[int] = Field(default=0, alias="Inventory")
    sales_group: str = Field(default="N/A", alias="SalesGroup")
    active: bool = Field(default=True, alias="Active")

    class Config:
        extra = "allow"
        populate_by_name = True


class HotelInfo(BaseModel):
    """Hotel information from the API response."""

    hotel_id: int = Field(alias="HotelId")
    hotel_code: str = Field(alias="HotelCode")
    hotel_name: str = Field(alias="HotelName")
    hotel_name_2: Optional[str] = Field(None, alias="HotelName2")
    fiscal_number: Optional[str] = Field(None, alias="FiscalNumber")
    hotel_date: Optional[datetime] = Field(None, alias="HotelDate")
    local_time: Optional[datetime] = Field(None, alias="LocalTime")
    hotel_email: Optional[str] = Field(None, alias="HotelEmail")
    start: Optional[datetime] = Field(None, alias="Start")
    end: Optional[datetime] = Field(None, alias="End")
    duration: Optional[float] = Field(None, alias="Duration")

    class Config:
        extra = "allow"
        populate_by_name = True


class HotelConfigResponse(BaseModel):
    """Complete hotel configuration response from Host PMS API /config endpoint.

    Maps to the actual API response structure with ConfigInfo array and HotelInfo object.
    """

    config_info: list[ConfigItem] = Field(default_factory=list, alias="ConfigInfo")
    hotel_info: HotelInfo = Field(alias="HotelInfo")

    class Config:
        extra = "allow"
        populate_by_name = True

    def get_config_by_type(self, config_type: str) -> list[ConfigItem]:
        """Filter configuration items by type.

        Args:
            config_type: Type to filter (e.g., "CATEGORY", "SEGMENT", "DIST CHANNEL")

        Returns:
            List of matching ConfigItem objects
        """
        return [item for item in self.config_info if item.config_type == config_type]

    @property
    def rooms(self) -> list[ConfigItem]:
        """Get all room categories."""
        return self.get_config_by_type("CATEGORY")

    @property
    def segments(self) -> list[ConfigItem]:
        """Get all segments."""
        return self.get_config_by_type("SEGMENT")

    @property
    def sub_segments(self) -> list[ConfigItem]:
        """Get all sub-segments."""
        return self.get_config_by_type("SUB-SEGMENT")

    @property
    def channels(self) -> list[ConfigItem]:
        """Get all distribution channels."""
        return self.get_config_by_type("DIST CHANNEL")

    @property
    def packages(self) -> list[ConfigItem]:
        """Get all packages."""
        return self.get_config_by_type("PACKAGE")

    @property
    def price_lists(self) -> list[ConfigItem]:
        """Get all price lists."""
        return self.get_config_by_type("PRICELIST")

    @property
    def charges(self) -> list[ConfigItem]:
        """Get all charges."""
        return self.get_config_by_type("CHARGE")

    @property
    def reservation_statuses(self) -> list[ConfigItem]:
        """Get all reservation statuses."""
        return self.get_config_by_type("RESERVATION STATUS")
