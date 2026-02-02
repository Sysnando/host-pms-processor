"""Unit tests for data transformers."""

import pytest

from src.models.host.config import ConfigItem, HotelConfigResponse, HotelInfo
from src.models.host.reservation import Reservation, RoomStay
from src.transformers import (
    ConfigTransformer,
    ReservationTransformer,
    SegmentTransformer,
)


class TestConfigTransformer:
    """Tests for ConfigTransformer."""

    def test_transform_basic_config(self):
        """Test transforming a basic hotel config with ConfigInfo structure."""
        config_data = {
            "ConfigInfo": [
                {
                    "ConfigType": "CATEGORY",
                    "ConfigId": 1,
                    "Code": "DOUBLE",
                    "Description": "Double Room",
                    "Inventory": 5,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
            ],
            "HotelInfo": {
                "HotelId": 1,
                "HotelCode": "HOTEL001",
                "HotelName": "Sample Hotel",
            },
        }

        climber_config, segments = ConfigTransformer.transform(config_data)

        assert climber_config.hotel_code == "HOTEL001"
        assert climber_config.hotel_name == "Sample Hotel"
        assert climber_config.room_count == 1
        assert len(climber_config.rooms) == 1
        assert climber_config.rooms[0].code == "DOUBLE"

    def test_transform_config_with_all_types(self):
        """Test transforming config with all segment types."""
        config_data = {
            "ConfigInfo": [
                {
                    "ConfigType": "CATEGORY",
                    "ConfigId": 1,
                    "Code": "DOUBLE",
                    "Description": "Double Room",
                    "Inventory": 5,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "SEGMENT",
                    "ConfigId": 2,
                    "Code": "EMPRESA",
                    "Description": "Empresa",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "SUB-SEGMENT",
                    "ConfigId": 3,
                    "Code": "LAZER",
                    "Description": "Lazer",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "DIST CHANNEL",
                    "ConfigId": 4,
                    "Code": "SITE",
                    "Description": "Site",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "PACKAGE",
                    "ConfigId": 5,
                    "Code": "AP",
                    "Description": "Accommodation",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "PRICELIST",
                    "ConfigId": 6,
                    "Code": "BALCAO",
                    "Description": "Balcao",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
            ],
            "HotelInfo": {
                "HotelId": 1,
                "HotelCode": "HOTEL001",
                "HotelName": "Hotel Demo",
            },
        }

        climber_config, segments = ConfigTransformer.transform(config_data)

        assert climber_config.hotel_code == "HOTEL001"
        assert climber_config.room_count == 1
        assert len(segments.segments) == 1  # SEGMENT type
        assert len(segments.sub_segments) == 1  # SUB-SEGMENT type
        assert len(segments.channels) == 1  # DIST CHANNEL
        assert len(segments.packages) == 1  # PACKAGE
        assert len(segments.rates) == 1  # PRICELIST maps to rates


class TestSegmentTransformer:
    """Tests for SegmentTransformer."""

    def test_transform_segments_by_type(self):
        """Test transforming segments with correct categorization."""
        segments = [
            {"code": "AIRBNB", "name": "Airbnb", "type": "channel"},
            {"code": "BOOKING", "name": "Booking", "type": "ota"},
            {"code": "AGENT1", "name": "Travel Agency", "type": "agency"},
            {"code": "CORP1", "name": "Corporation", "type": "company"},
        ]

        collection = SegmentTransformer.transform(segments)

        # Note: "ota" should default to segments since it's not in mapping
        assert len(collection.channels) == 1
        assert len(collection.agencies) == 1
        assert len(collection.companies) == 1
        assert collection.channels[0].code == "AIRBNB"
        assert collection.agencies[0].code == "AGENT1"
        assert collection.companies[0].code == "CORP1"

    def test_transform_unmapped_segment_type(self):
        """Test that unmapped segment types go to default category."""
        segments = [
            {"code": "UNKNOWN", "name": "Unknown Type", "type": "custom_type"},
        ]

        collection = SegmentTransformer.transform(segments)

        # Should be in segments (default category)
        assert len(collection.segments) == 1
        assert collection.segments[0].code == "UNKNOWN"

    def test_merge_segment_collections(self):
        """Test merging multiple segment collections."""
        collection1 = SegmentTransformer.transform(
            [
                {"code": "CH1", "name": "Channel 1", "type": "channel"},
            ]
        )

        collection2 = SegmentTransformer.transform(
            [
                {"code": "CH2", "name": "Channel 2", "type": "channel"},
                {"code": "AG1", "name": "Agency 1", "type": "agency"},
            ]
        )

        merged = SegmentTransformer.merge_segment_collections(
            [collection1, collection2]
        )

        assert len(merged.channels) == 2
        assert len(merged.agencies) == 1


class TestReservationTransformer:
    """Tests for ReservationTransformer."""

    def test_transform_active_reservation(self):
        """Test transforming an active reservation."""
        from datetime import datetime

        room_stay = RoomStay(
            room_id="ROOM001",
            room_code="DOUBLE",
            check_in_date=datetime(2024, 1, 15),
            check_out_date=datetime(2024, 1, 17),
            rate_amount=100.0,
            total_amount=200.0,
        )

        reservation = Reservation(
            reservation_id="RES001",
            hotel_code="HOTEL001",
            confirmation_number="CONF123",
            status="ACTIVE",
            room_stays=[room_stay],
            total_revenue=200.0,
        )

        climber_res = ReservationTransformer.transform(reservation)

        assert climber_res.reservation_id == "RES001"
        assert climber_res.status == "ACTIVE"
        assert climber_res.number_of_nights == 2
        assert climber_res.revenue.total_revenue == 200.0

    def test_transform_cancelled_reservation_zeros_revenue(self):
        """Test that cancelled reservations have zero revenue."""
        from datetime import datetime

        room_stay = RoomStay(
            room_id="ROOM001",
            room_code="DOUBLE",
            check_in_date=datetime(2024, 1, 15),
            check_out_date=datetime(2024, 1, 17),
            rate_amount=100.0,
            total_amount=200.0,
        )

        reservation = Reservation(
            reservation_id="RES002",
            hotel_code="HOTEL001",
            status="CANCELLED",
            room_stays=[room_stay],
            total_revenue=200.0,
        )

        climber_res = ReservationTransformer.transform(reservation)

        assert climber_res.status == "CANCELLED"
        assert climber_res.revenue.total_revenue == 0.0

    def test_transform_status_mapping(self):
        """Test status mapping from Host to Climber format."""
        test_cases = [
            ("ACTIVE", "ACTIVE"),
            ("CONFIRMED", "ACTIVE"),
            ("CANCELLED", "CANCELLED"),
            ("CHECKED_IN", "CHECKED_IN"),
            ("CHECKED_OUT", "CHECKED_OUT"),
            ("DEPARTU", "ACTIVE"),  # Unknown status defaults to ACTIVE
        ]

        for host_status, expected_climber_status in test_cases:
            mapped = ReservationTransformer._map_status(host_status)
            assert mapped == expected_climber_status

    def test_transform_batch_reservations(self):
        """Test batch transformation of reservations."""
        from datetime import datetime

        reservations = [
            {
                "reservationId": "RES001",
                "hotelCode": "HOTEL001",
                "status": "ACTIVE",
                "roomStays": [
                    {
                        "roomId": "R1",
                        "roomCode": "D",
                        "checkInDate": "2024-01-15",
                        "checkOutDate": "2024-01-17",
                        "totalAmount": 200.0,
                    }
                ],
                "totalRevenue": 200.0,
            },
            {
                "reservationId": "RES002",
                "hotelCode": "HOTEL001",
                "status": "CANCELLED",
                "roomStays": [],
                "totalRevenue": 0.0,
            },
        ]

        collection = ReservationTransformer.transform_batch(reservations)

        assert collection.total_count == 2
        assert len(collection.reservations) == 2
        assert collection.reservations[0].status == "ACTIVE"
        assert collection.reservations[1].status == "CANCELLED"


class TestTransformerEdgeCases:
    """Tests for edge cases and error handling."""

    def test_transform_empty_config_info(self):
        """Test transforming config with empty ConfigInfo."""
        config_data = {
            "ConfigInfo": [],
            "HotelInfo": {
                "HotelId": 1,
                "HotelCode": "HOTEL_EMPTY",
                "HotelName": "Empty Hotel",
            },
        }

        climber_config, segments = ConfigTransformer.transform(config_data)

        assert climber_config.hotel_code == "HOTEL_EMPTY"
        assert climber_config.room_count == 0
        assert len(climber_config.rooms) == 0

    def test_transform_invalid_config_raises_error(self):
        """Test that invalid config data raises appropriate error."""
        invalid_config = {"ConfigInfo": []}  # Missing HotelInfo

        with pytest.raises(ValueError):
            ConfigTransformer.transform(invalid_config)

    def test_get_reservation_statuses(self):
        """Test extracting reservation statuses from config."""
        config_data = {
            "ConfigInfo": [
                {
                    "ConfigType": "RESERVATION STATUS",
                    "ConfigId": 1,
                    "Code": "STANDARD",
                    "Description": "Confirmed",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
                {
                    "ConfigType": "RESERVATION STATUS",
                    "ConfigId": 2,
                    "Code": "CXL",
                    "Description": "Cancellation",
                    "Inventory": 0,
                    "SalesGroup": "N/A",
                    "Active": True,
                },
            ],
            "HotelInfo": {
                "HotelId": 1,
                "HotelCode": "HOTEL001",
                "HotelName": "Hotel Demo",
            },
        }

        config_response = HotelConfigResponse(**config_data)
        status_map = ConfigTransformer.get_reservation_statuses(config_response)

        assert "STANDARD" in status_map
        assert status_map["STANDARD"] == "Confirmed"
        assert status_map["CXL"] == "Cancellation"

    def test_get_charges(self):
        """Test extracting charges from config."""
        config_data = {
            "ConfigInfo": [
                {
                    "ConfigType": "CHARGE",
                    "ConfigId": 1,
                    "Code": "ALOJ",
                    "Description": "Alojamento",
                    "Inventory": 0,
                    "SalesGroup": "ROOM",
                    "Active": True,
                },
            ],
            "HotelInfo": {
                "HotelId": 1,
                "HotelCode": "HOTEL001",
                "HotelName": "Hotel Demo",
            },
        }

        config_response = HotelConfigResponse(**config_data)
        charges = ConfigTransformer.get_charges(config_response)

        assert len(charges) == 1
        assert charges[0].code == "ALOJ"
        assert charges[0].name == "Alojamento"
