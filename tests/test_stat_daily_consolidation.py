"""Test StatDaily consolidation logic."""

from datetime import datetime
from src.transformers.stat_daily_transformer import StatDailyTransformer


def test_consolidation_basic():
    """Test basic consolidation of HISTORY-REVENUE and HISTORY-OCCUPANCY records."""
    # Simulate two records for same reservation on same day
    records = [
        {
            "RowNumber": 1,
            "TotalRows": 2,
            "RecordType": "HISTORY-REVENUE",
            "HotelDate": "2025-01-15T00:00:00",
            "ResNo": 12345,
            "ResId": 67890,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 111,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "ALOJ",
            "RevenueNet": 100.50,
            "RevenueGross": 120.00,
        },
        {
            "RowNumber": 2,
            "TotalRows": 2,
            "RecordType": "HISTORY-OCCUPANCY",
            "HotelDate": "2025-01-15T00:00:00",
            "ResNo": 12345,
            "ResId": 67890,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 111,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "ALOJ",
            "RevenueNet": 0.0,  # Occupancy record has no revenue
            "RevenueGross": 0.0,
        },
    ]

    consolidated = StatDailyTransformer.consolidate_stat_daily_records(records)

    # Should consolidate to single record
    assert len(consolidated) == 1, f"Expected 1 consolidated record, got {len(consolidated)}"

    record = consolidated[0]
    assert record["res_no"] == 12345
    assert record["res_id"] == 67890
    assert record["charge_code"] == "ALOJ"
    assert record["global_res_guest_id"] == 111
    assert record["revenue_net"] == 100.50  # From HISTORY-REVENUE
    # Date should be from HISTORY-OCCUPANCY (same in this case)
    assert "2025-01-15" in str(record["hotel_date"])

    print("✅ test_consolidation_basic passed")


def test_consolidation_date_preference():
    """Test that HotelDate from HISTORY-OCCUPANCY is preferred."""
    # Edge case: HISTORY-REVENUE and HISTORY-OCCUPANCY have different dates
    records = [
        {
            "RowNumber": 1,
            "TotalRows": 2,
            "RecordType": "HISTORY-REVENUE",
            "HotelDate": "2025-01-14T23:30:00",  # Different date/time
            "ResNo": 12345,
            "ResId": 67890,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 111,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "ALOJ",
            "RevenueNet": 100.50,
            "RevenueGross": 120.00,
        },
        {
            "RowNumber": 2,
            "TotalRows": 2,
            "RecordType": "HISTORY-OCCUPANCY",
            "HotelDate": "2025-01-15T00:00:00",  # Correct date
            "ResNo": 12345,
            "ResId": 67890,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 111,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "ALOJ",
            "RevenueNet": 0.0,
            "RevenueGross": 0.0,
        },
    ]

    consolidated = StatDailyTransformer.consolidate_stat_daily_records(records)

    assert len(consolidated) == 1
    record = consolidated[0]

    # Should use date from HISTORY-OCCUPANCY (2025-01-15), not HISTORY-REVENUE (2025-01-14)
    hotel_date_str = str(record["hotel_date"])
    assert "2025-01-15" in hotel_date_str, f"Expected 2025-01-15, got {hotel_date_str}"

    print("✅ test_consolidation_date_preference passed")


def test_aggregation_regular_vs_noshow():
    """Test dual aggregation strategy for regular charges vs NOSHOW."""
    consolidated_records = [
        {
            "hotel_date": "2025-01-15T00:00:00",
            "res_no": 12345,
            "res_id": 67890,
            "charge_code": "ALOJ",
            "global_res_guest_id": 111,
            "revenue_net": 100.50,
        },
        {
            "hotel_date": "2025-01-15T00:00:00",
            "res_no": 12345,
            "res_id": 67890,
            "charge_code": "NOSHOW",
            "global_res_guest_id": 111,  # Will be ignored for NOSHOW
            "revenue_net": 50.00,
        },
    ]

    regular_map, noshow_map = StatDailyTransformer.aggregate_revenue_by_key(
        consolidated_records
    )

    # Regular charge should use full key with GlobalResGuestId
    regular_key = ("2025-01-15", 12345, 111, 67890)
    assert regular_key in regular_map, f"Regular key not found: {regular_key}"
    assert regular_map[regular_key] == 100.50

    # NOSHOW should use only (HotelDate, ResId)
    noshow_key = ("2025-01-15", 67890)
    assert noshow_key in noshow_map, f"NOSHOW key not found: {noshow_key}"
    assert noshow_map[noshow_key] == 50.00

    print("✅ test_aggregation_regular_vs_noshow passed")


def test_filtering_invalid_charge_codes():
    """Test that only ALOJ, NOSHOW, OB are processed."""
    records = [
        {
            "RowNumber": 1,
            "TotalRows": 3,
            "RecordType": "HISTORY-REVENUE",
            "HotelDate": "2025-01-15T00:00:00",
            "ResNo": 12345,
            "ResId": 67890,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 111,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "ALOJ",
            "RevenueNet": 100.50,
            "RevenueGross": 120.00,
        },
        {
            "RowNumber": 2,
            "TotalRows": 3,
            "RecordType": "HISTORY-REVENUE",
            "HotelDate": "2025-01-15T00:00:00",
            "ResNo": 12345,
            "ResId": 67891,
            "DetailId": 2,
            "MasterDetail": 0,
            "GlobalResGuestId": 112,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "OTHER",  # Invalid charge code - should be filtered
            "RevenueNet": 50.00,
            "RevenueGross": 60.00,
        },
        {
            "RowNumber": 3,
            "TotalRows": 3,
            "RecordType": "HISTORY-REVENUE",
            "HotelDate": "2025-01-15T00:00:00",
            "ResNo": 12345,
            "ResId": 67892,
            "DetailId": 3,
            "MasterDetail": 0,
            "GlobalResGuestId": 113,
            "CreatedOn": "2025-01-10T10:00:00",
            "CheckIn": "2025-01-15T14:00:00",
            "CheckOut": "2025-01-16T11:00:00",
            "ResStatus": 1,
            "SalesGroup": 0,
            "ChargeCode": "NOSHOW",
            "RevenueNet": 30.00,
            "RevenueGross": 30.00,
        },
    ]

    consolidated = StatDailyTransformer.consolidate_stat_daily_records(records)

    # Should only have ALOJ and NOSHOW (OTHER filtered out)
    assert len(consolidated) == 2, f"Expected 2 records, got {len(consolidated)}"

    charge_codes = {r["charge_code"] for r in consolidated}
    assert charge_codes == {"ALOJ", "NOSHOW"}

    print("✅ test_filtering_invalid_charge_codes passed")


if __name__ == "__main__":
    test_consolidation_basic()
    test_consolidation_date_preference()
    test_aggregation_regular_vs_noshow()
    test_filtering_invalid_charge_codes()
    print("\n✅ All StatDaily consolidation tests passed!")
