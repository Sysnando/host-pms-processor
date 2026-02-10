"""Pydantic models for Host PMS API StatDaily responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class StatDailyRecord(BaseModel):
    """Single StatDaily record from Host PMS API.

    Contains daily statistical information including occupancy and revenue
    for hotel reservations. The API returns multiple entries per reservation
    with different RecordTypes and ChargeCodes.
    """

    row_number: int = Field(alias="RowNumber")
    total_rows: int = Field(alias="TotalRows")
    record_type: str = Field(alias="RecordType")  # e.g., "HISTORY-REVENUE", "HISTORY-OCCUPANCY", "FORECAST-REVENUE", "FORECAST-OCCUPANCY"
    hotel_date: datetime = Field(alias="HotelDate")
    res_no: int = Field(alias="ResNo")
    res_id: int = Field(alias="ResId")
    detail_id: int = Field(alias="DetailId")
    master_detail: int = Field(alias="MasterDetail")
    global_res_guest_id: int = Field(alias="GlobalResGuestId")
    created_on: datetime = Field(alias="CreatedOn")
    check_in: datetime = Field(alias="CheckIn")
    check_out: datetime = Field(alias="CheckOut")
    option_date: Optional[datetime] = Field(None, alias="OptionDate")
    category: Optional[str] = Field(None, alias="Category")
    complex_code: Optional[str] = Field(None, alias="ComplexCode")
    room_name: Optional[str] = Field(None, alias="RoomName")
    agency: Optional[str] = Field(None, alias="Agency")
    company: Optional[str] = Field(None, alias="Company")
    cro: Optional[str] = Field(None, alias="Cro")
    groupname: Optional[str] = Field(None, alias="Groupname")
    res_status: int = Field(alias="ResStatus")
    guest_id: Optional[int] = Field(None, alias="Guest_Id")
    country_iso_code: Optional[str] = Field(None, alias="CountryIsoCode")
    nationality_iso_code: Optional[str] = Field(None, alias="NationalityIsoCode")
    pack: Optional[str] = Field(None, alias="Pack")
    price_list: Optional[str] = Field(None, alias="PriceList")
    segment_description: Optional[str] = Field(None, alias="SegmentDescription")
    sub_segment_description: Optional[str] = Field(None, alias="SubSegmentDescription")
    channel_description: Optional[str] = Field(None, alias="ChannelDescription")
    additional_status_code: Optional[str] = Field(None, alias="AdditionalStatusCode")
    additional_status_description: Optional[str] = Field(None, alias="AdditionalStatusDescription")
    category_upgrade_from: Optional[str] = Field(None, alias="CategoryUpgradeFrom")
    pax: int = Field(default=0, alias="Pax")
    children_type1: int = Field(default=0, alias="ChildrenType1")
    children_type2: int = Field(default=0, alias="ChildrenType2")
    children_type3: int = Field(default=0, alias="ChildrenType3")
    room_nights: int = Field(default=0, alias="RoomNights")
    charge_code: Optional[str] = Field(None, alias="ChargeCode")
    sales_group: int = Field(alias="SalesGroup")
    sales_group_desc: Optional[str] = Field(None, alias="SalesGroupDesc")
    revenue_gross: float = Field(default=0.0, alias="RevenueGross")
    revenue_net: float = Field(default=0.0, alias="RevenueNet")

    class Config:
        extra = "allow"
        populate_by_name = True


class StatDailyResponse(BaseModel):
    """StatDaily response from Host PMS API.

    Contains a list of daily statistical records for a hotel.
    """

    records: list[StatDailyRecord] = Field(default_factory=list)
    total_count: Optional[int] = None
    hotel_code: Optional[str] = None
    fetch_date: Optional[datetime] = None

    class Config:
        extra = "allow"
        populate_by_name = True
