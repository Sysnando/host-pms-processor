"""Pydantic models for Climber standardized reservation format."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ClimberReservation(BaseModel):
    """Climber standardized reservation format - exact mapping required.

    This model follows Climber's exact JSON specification with no additional fields.
    All date fields use ISO 8601 format.
    Status is stored as integer (0-5) instead of string.
    All segment codes default to "UNASSIGNED" if not provided by Host PMS.
    """

    record_date: str = Field(
        alias="recordDate",
        description="Date range that this reservation state is valid, e.g., '[2021-06-02,)'",
    )
    calendar_date: str = Field(
        alias="calendarDate",
        description="The day of stay (ISO format date)",
    )
    calendar_date_start: str = Field(
        alias="calendarDateStart",
        description="The first stay day of this reservation (ISO format date)",
    )
    calendar_date_end: str = Field(
        alias="calendarDateEnd",
        description="The last stay day of this reservation (ISO format date)",
    )
    created_date: str = Field(
        alias="createdDate",
        description="The day that this reservation was created (ISO format date)",
    )
    pax: int = Field(description="Number of persons in the room")
    reservation_id: str = Field(
        alias="reservationId",
        description="Reservation ID (internal, as string)",
    )
    reservation_id_external: str = Field(
        alias="reservationIdExternal",
        description="Reservation ID in the PMS (external, as string)",
    )
    revenue_fb: float = Field(
        default=0.0,
        alias="revenueFb",
        description="F&B value",
    )
    revenue_fb_invoice: float = Field(
        default=0.0,
        alias="revenueFbInvoice",
        description="F&B value invoiced (only valid for past stays at checkout)",
    )
    revenue_others: float = Field(
        default=0.0,
        alias="revenueOthers",
        description="Other values in the reservation",
    )
    revenue_others_invoice: float = Field(
        default=0.0,
        alias="revenueOthersInvoice",
        description="Other values invoiced (only valid for past stays at checkout)",
    )
    revenue_room: float = Field(
        default=0.0,
        alias="revenueRoom",
        description="Room value",
    )
    revenue_room_invoice: float = Field(
        default=0.0,
        alias="revenueRoomInvoice",
        description="Room value invoiced (only valid for past stays at checkout)",
    )
    rooms: int = Field(
        description="Number of rooms in the same reservation",
    )
    status: int = Field(
        description="Status code: 0=CANCELLED, 1=CHECKED_IN, 2=CHECKED_OUT, 3=CONFIRMED, 4=NO_SHOW, 5=TENTATIVE",
    )
    agency_code: str = Field(
        alias="agencyCode",
        description="Agency segment code (UNASSIGNED if not supported)",
    )
    channel_code: str = Field(
        alias="channelCode",
        description="Channel segment code (UNASSIGNED if not supported)",
    )
    company_code: str = Field(
        alias="companyCode",
        description="Company segment code (UNASSIGNED if not supported)",
    )
    cro_code: str = Field(
        alias="croCode",
        description="CRO segment code (UNASSIGNED if not supported)",
    )
    group_code: str = Field(
        alias="groupCode",
        description="Group segment code (UNASSIGNED if not supported)",
    )
    package_code: str = Field(
        alias="packageCode",
        description="Package segment code (UNASSIGNED if not supported)",
    )
    rate_code: str = Field(
        alias="rateCode",
        description="Rate segment code (UNASSIGNED if not supported)",
    )
    room_code: str = Field(
        alias="roomCode",
        description="Room segment code (UNASSIGNED if not supported)",
    )
    segment_code: str = Field(
        alias="segmentCode",
        description="Segment code (UNASSIGNED if not supported)",
    )
    sub_segment_code: str = Field(
        alias="subSegmentCode",
        description="Sub-segment code (UNASSIGNED if not supported)",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Reject any extra fields not in this model
        alias_priority="alias",  # Use alias names for serialization
    )


class ReservationCollection(BaseModel):
    """Collection of reservations in Climber standardized format."""

    reservations: list[ClimberReservation] = Field(
        default_factory=list,
        description="Array of reservation objects",
    )

    @property
    def total_count(self) -> int:
        """Get total count of reservations in collection.

        Returns:
            Number of reservations in the collection
        """
        return len(self.reservations)

    class Config:
        extra = "allow"  # Allow hotel_code if passed during creation
