"""Host PMS StatSummary model."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class StatSummaryRecord(BaseModel):
    """Model for a single StatSummary record from Host PMS API.

    This endpoint provides daily aggregated statistics for validation purposes.
    It shows total room nights and revenue per day.
    """

    hotel_date: datetime = Field(..., alias="hoteldate")
    room_nights: int = Field(..., alias="RoomNights")
    revenue_net_room: float = Field(..., alias="RevenueNet_Room")
    revenue_net_other: float = Field(..., alias="RevenueNet_Other")
    checksum: Optional[int] = Field(None, alias="Checksum")

    class Config:
        """Pydantic configuration."""

        populate_by_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }
