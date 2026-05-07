"""Pydantic models for Host PMS API reservation responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Guest(BaseModel):
    """Guest information from Host PMS API."""

    guest_id: int = Field(alias="GuestId")
    guest_no: int = Field(alias="GuestNo")
    name_formatted: str = Field(alias="NameFormatted")
    sex: int = Field(default=-1, alias="Sex")
    country_iso_code: str | None = Field(None, alias="CountryIsoCode")
    birth_date: datetime | None = Field(None, alias="BirthDate")
    zip_code: str | None = Field(None, alias="ZipCode")
    nationality_iso_code: str | None = Field(None, alias="NationalityIsoCode")
    email_1: str | None = Field(None, alias="Email1")
    email_2: str | None = Field(None, alias="Email2")
    global_res_guest_id: int | None = Field(None, alias="GlobalResguestId")

    @field_validator("birth_date", mode="before")
    @classmethod
    def parse_birth_date(cls, v):
        """Parse date-only strings to datetime."""
        if isinstance(v, str) and v and len(v) == 10:  # YYYY-MM-DD format
            try:
                return datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                pass
        return v

    class Config:
        extra = "allow"
        populate_by_name = True


class PriceItem(BaseModel):
    """Price/charge line item in reservation."""

    global_res_guest_id: int = Field(alias="GlobalResguestId")
    sales_group: int = Field(alias="SalesGroup")  # 0=Room, 1=F&B, etc.
    sales_group_desc: str = Field(alias="SalesGroupDesc")
    date: datetime = Field(alias="Date")
    charge: str = Field(alias="Charge")
    amount: float = Field(alias="Amount")
    pax_type: int = Field(default=0, alias="PaxType")
    pax_type_desc: str = Field(alias="PaxTypeDesc")

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v):
        """Parse date-only strings to datetime."""
        if isinstance(v, str) and v and len(v) == 10:  # YYYY-MM-DD format
            try:
                return datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                pass
        return v

    class Config:
        extra = "allow"
        populate_by_name = True


class HostReservation(BaseModel):
    """Reservation from Host PMS API response."""

    res_no: int = Field(alias="ResNo")  # Reservation number
    res_id: int = Field(alias="ResId")  # Reservation ID
    detail_id: int = Field(alias="DetailId")
    master_detail: int = Field(default=0, alias="MasterDetail")
    global_res_guest_id: int = Field(alias="GlobalResGuestId")
    created_on: datetime = Field(alias="CreatedOn")
    last_update: datetime = Field(alias="LastUpdate")
    check_in: datetime = Field(alias="CheckIn")
    check_out: datetime = Field(alias="CheckOut")
    option_date: datetime | None = Field(None, alias="OptionDate")
    rooms: int = Field(default=1, alias="Rooms")
    category: str = Field(alias="Category")  # Room type/category code
    agency: str = Field(alias="Agency")  # Agency name
    company: str | None = Field(None, alias="Company")
    cro: str | None = Field(None, alias="Cro")  # Customer Relations Organization
    group_name: str | None = Field(None, alias="GroupName")
    res_status: int = Field(
        alias="ResStatus"
    )  # 0=CANCELLED, 1=CHECKED_IN, 2=CHECKED_OUT, 3=CONFIRMED, 4=NO_SHOW, 5=TENTATIVE
    guest_id: int = Field(alias="GuestId")
    pax: int = Field(alias="Pax")  # Number of persons
    pack: str | None = Field(None, alias="Pack")  # Package code
    price_list: str = Field(alias="PriceList")
    segment_description: str = Field(alias="SegmentDescription")
    sub_segment_description: str = Field(alias="SubSegmentDescription")
    channel_description: str = Field(alias="ChannelDescription")
    children_type_1: int = Field(default=0, alias="ChildrenType1")
    children_type_2: int = Field(default=0, alias="ChildrenType2")
    children_type_3: int = Field(default=0, alias="ChildrenType3")
    guests: list[Guest] = Field(default_factory=list, alias="Guests")
    prices: list[PriceItem] = Field(default_factory=list, alias="Prices")
    row_number: int | None = Field(None, alias="RowNumber")
    total_rows: int | None = Field(None, alias="TotalRows")

    class Config:
        extra = "allow"
        populate_by_name = True

    def get_revenue_by_sales_group(self) -> dict[int, float]:
        """Calculate total revenue by sales group.

        Returns:
            Dictionary mapping sales_group to total amount
        """
        revenue = {}
        for price in self.prices:
            if price.sales_group not in revenue:
                revenue[price.sales_group] = 0.0
            revenue[price.sales_group] += price.amount
        return revenue

    @property
    def room_revenue(self) -> float:
        """Get total room revenue (SalesGroup 0)."""
        return self.get_revenue_by_sales_group().get(0, 0.0)

    @property
    def fb_revenue(self) -> float:
        """Get total F&B revenue (SalesGroup 1)."""
        return self.get_revenue_by_sales_group().get(1, 0.0)

    @property
    def other_revenue(self) -> float:
        """Get total other revenue (SalesGroup > 1)."""
        revenue_map = self.get_revenue_by_sales_group()
        return sum(v for k, v in revenue_map.items() if k > 1)

    @property
    def total_revenue(self) -> float:
        """Get total revenue from all charges."""
        return sum(self.get_revenue_by_sales_group().values())


class ReservationResponse(BaseModel):
    """Reservation list response from Host PMS API."""

    reservations: list[HostReservation] = Field(alias="Reservations")

    class Config:
        extra = "allow"
        populate_by_name = True


# Legacy models for compatibility (can be deprecated)


class GuestInfo(BaseModel):
    """Legacy: Guest information."""

    first_name: str | None = Field(None, alias="firstName")
    last_name: str | None = Field(None, alias="lastName")
    email: str | None = None
    phone: str | None = None

    class Config:
        extra = "allow"
        populate_by_name = True


class RoomStay(BaseModel):
    """Legacy: Room stay details within a reservation."""

    room_id: str = Field(alias="roomId")
    room_code: str | None = Field(None, alias="roomCode")
    room_type_code: str | None = Field(None, alias="roomTypeCode")
    check_in_date: datetime = Field(alias="checkInDate")
    check_out_date: datetime = Field(alias="checkOutDate")
    rate_code: str | None = Field(None, alias="rateCode")
    rate_plan: str | None = Field(None, alias="ratePlan")
    rate_amount: float = Field(default=0.0, alias="rateAmount")
    total_amount: float = Field(default=0.0, alias="totalAmount")
    number_of_nights: int | None = Field(None, alias="numberOfNights")
    number_of_guests: int | None = Field(None, alias="numberOfGuests")

    class Config:
        extra = "allow"
        populate_by_name = True


class Reservation(BaseModel):
    """Legacy: Reservation response from Host PMS API."""

    reservation_id: str = Field(alias="reservationId")
    hotel_code: str = Field(alias="hotelCode")
    confirmation_number: str | None = Field(None, alias="confirmationNumber")
    guest: GuestInfo = Field(default_factory=GuestInfo)
    room_stays: list[RoomStay] = Field(default_factory=list, alias="roomStays")
    booking_date: datetime | None = Field(None, alias="bookingDate")
    status: str = Field(default="ACTIVE")  # ACTIVE, CANCELLED, CHECKED_IN, CHECKED_OUT
    source: str | None = None  # OTA, DIRECT, AGENCY, etc.
    market_segment: str | None = Field(None, alias="marketSegment")
    segment: str | None = None  # Segment code (agency, channel, company, etc.)
    total_cost: float = Field(default=0.0, alias="totalCost")
    total_revenue: float = Field(default=0.0, alias="totalRevenue")
    cancellation_date: datetime | None = Field(None, alias="cancellationDate")
    cancellation_reason: str | None = Field(None, alias="cancellationReason")
    last_modified: datetime | None = Field(None, alias="lastModified")

    class Config:
        extra = "allow"
        populate_by_name = True
