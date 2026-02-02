"""Pydantic models for Climber standardized segment format."""

from typing import Optional

from pydantic import BaseModel, Field


class SegmentItem(BaseModel):
    """Climber segment/configuration item with occupancy and revenue settings."""

    code: str = Field(description="Unique segment code, same as used in reservations")
    name: str = Field(description="Segment name/description")
    enabled_otb: bool = Field(
        default=True,
        alias="enabledOtb",
        description="Does this segment affect occupancy (OTB)?",
    )
    enabled_revenue: bool = Field(
        default=True,
        alias="enabledRevenue",
        description="Does this segment affect revenue?",
    )
    position: int = Field(
        default=9999,
        description="Sorting order position, 9999 is default",
    )
    description: Optional[str] = Field(None, description="Additional description")
    type: Optional[str] = Field(None, description="Segment type for internal tracking")

    class Config:
        extra = "allow"
        populate_by_name = True


class SegmentCollection(BaseModel):
    """Collection of segments organized by type, matching Climber expected format."""

    agencies: list[SegmentItem] = Field(
        default_factory=list, description="Agency segments"
    )
    channels: list[SegmentItem] = Field(
        default_factory=list, description="Distribution channel segments"
    )
    companies: list[SegmentItem] = Field(
        default_factory=list, description="Company segments"
    )
    cros: list[SegmentItem] = Field(
        default_factory=list, description="Customer Relations Organization segments"
    )
    groups: list[SegmentItem] = Field(
        default_factory=list, description="Group segments"
    )
    packages: list[SegmentItem] = Field(
        default_factory=list, description="Package segments"
    )
    rates: list[SegmentItem] = Field(
        default_factory=list, description="Rate plan segments (price lists)"
    )
    rooms: list[SegmentItem] = Field(
        default_factory=list, description="Room type segments (categories)"
    )
    segments: list[SegmentItem] = Field(
        default_factory=list, description="Market segment segments"
    )
    sub_segments: list[SegmentItem] = Field(
        default_factory=list,
        alias="subSegments",
        description="Sub-segment segments",
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
            "agencies": [item.model_dump(by_alias=True) for item in self.agencies],
            "channels": [item.model_dump(by_alias=True) for item in self.channels],
            "companies": [item.model_dump(by_alias=True) for item in self.companies],
            "cros": [item.model_dump(by_alias=True) for item in self.cros],
            "groups": [item.model_dump(by_alias=True) for item in self.groups],
            "packages": [item.model_dump(by_alias=True) for item in self.packages],
            "rates": [item.model_dump(by_alias=True) for item in self.rates],
            "rooms": [item.model_dump(by_alias=True) for item in self.rooms],
            "segments": [item.model_dump(by_alias=True) for item in self.segments],
            "subSegments": [
                item.model_dump(by_alias=True) for item in self.sub_segments
            ],
        }
