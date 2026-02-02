"""
Complete end-to-end ETL flow testing with fixtures and mocked services.

This shows how to test your entire orchestration without hitting real APIs.
"""
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

import pytest

from src.config.logging import get_logger, configure_logging

# Configure logging at module level
configure_logging()
logger = get_logger(__name__)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(filename):
    """Helper to load a fixture file."""
    with open(FIXTURES_DIR / filename) as f:
        return json.load(f)


class TestETLFlowWithFixtures:
    """Test the complete ETL flow using fixtures and mocked services."""

    @pytest.mark.asyncio
    async def test_single_hotel_processing_happy_path(self):
        """
        Test processing a single hotel with all fixture data.

        This mocks:
        - Host PMS API responses
        - S3 uploads
        - SQS messages
        - ESB client calls
        """
        from src.services.orchestration_service import HostPMSConnectorOrchestrator

        # Load fixtures
        config_data = load_fixture("host_pms_api/config_response.json")
        reservation_data = load_fixture("host_pms_api/reservation_response.json")

        # Create orchestrator
        orchestrator = HostPMSConnectorOrchestrator()

        # Mock the Host API client
        orchestrator.host_api_client = AsyncMock()
        orchestrator.host_api_client.get_hotel_config = AsyncMock(
            return_value=config_data
        )
        orchestrator.host_api_client.get_reservations = AsyncMock(
            return_value=reservation_data
        )

        # Mock ESB client
        orchestrator.esb_client = AsyncMock()
        orchestrator.esb_client.get_hotel_parameters = AsyncMock(
            return_value={"lastImportDate": "2024-10-01"}
        )
        orchestrator.esb_client.register_file = AsyncMock()
        orchestrator.esb_client.update_import_date = AsyncMock()

        # Mock S3 manager
        orchestrator.s3_manager = MagicMock()
        orchestrator.s3_manager.upload_raw = MagicMock(
            return_value={
                "key": "raw/HOTEL001/config/2024-10-26.json",
                "url": "s3://bucket/raw/HOTEL001/config/2024-10-26.json"
            }
        )
        orchestrator.s3_manager.upload_processed = MagicMock(
            return_value={
                "key": "processed/HOTEL001/config/2024-10-26.json",
                "url": "s3://bucket/processed/HOTEL001/config/2024-10-26.json"
            }
        )

        # Mock SQS manager
        orchestrator.sqs_manager = MagicMock()
        orchestrator.sqs_manager.send_message = MagicMock(
            return_value={"message_id": "msg-123"}
        )

        # Run orchestration
        result = await orchestrator.process_hotel("HOTEL001")

        # Verify results
        assert result["success"] is True
        assert result["hotel_code"] == "HOTEL001"
        assert result["config"] is not None
        assert result["reservations"] is not None
        assert len(result["sqs_messages"]) > 0

        # Verify API calls were made
        orchestrator.host_api_client.get_hotel_config.assert_called_once()
        orchestrator.host_api_client.get_reservations.assert_called_once()

        # Verify S3 uploads were called
        assert orchestrator.s3_manager.upload_raw.called
        assert orchestrator.s3_manager.upload_processed.called

        # Verify ESB registration was called
        assert orchestrator.esb_client.register_file.called

        logger.info("Happy path test passed!",
                    config_rooms=result['config']['room_count'],
                    reservations=result['reservations']['record_count'],
                    sqs_messages=len(result['sqs_messages']))

    @pytest.mark.asyncio
    async def test_etl_with_cancelled_reservation(self):
        """Test handling of cancelled reservations."""
        from src.services.orchestration_service import HostPMSConnectorOrchestrator

        config_data = load_fixture("host_pms_api/config_response.json")
        cancelled_data = load_fixture("edge_cases/cancelled_reservation.json")

        orchestrator = HostPMSConnectorOrchestrator()

        # Mock clients
        orchestrator.host_api_client = AsyncMock()
        orchestrator.host_api_client.get_hotel_config = AsyncMock(
            return_value=config_data
        )
        orchestrator.host_api_client.get_reservations = AsyncMock(
            return_value=cancelled_data
        )

        orchestrator.esb_client = AsyncMock()
        orchestrator.esb_client.get_hotel_parameters = AsyncMock(
            return_value={"lastImportDate": "2024-10-01"}
        )
        orchestrator.esb_client.register_file = AsyncMock()
        orchestrator.esb_client.update_import_date = AsyncMock()

        orchestrator.s3_manager = MagicMock()
        orchestrator.s3_manager.upload_raw = MagicMock(
            return_value={"key": "raw/...", "url": "s3://..."}
        )
        orchestrator.s3_manager.upload_processed = MagicMock(
            return_value={"key": "processed/...", "url": "s3://..."}
        )

        orchestrator.sqs_manager = MagicMock()
        orchestrator.sqs_manager.send_message = MagicMock(
            return_value={"message_id": "msg-123"}
        )

        # Run orchestration
        result = await orchestrator.process_hotel("HOTEL001")

        # Cancelled reservation should still be processed
        assert result["success"] is True
        assert result["reservations"]["record_count"] == 1

        logger.info("Cancelled reservation test passed!",
                    processed_reservations=result['reservations']['record_count'])

    def test_fixture_data_structure(self):
        """Verify fixture data has correct structure."""
        config = load_fixture("host_pms_api/config_response.json")
        reservation = load_fixture("host_pms_api/reservation_response.json")
        inventory = load_fixture("host_pms_api/inventory_response.json")
        revenue = load_fixture("host_pms_api/revenue_response.json")

        # Config structure
        assert "HotelInfo" in config
        assert "ConfigInfo" in config
        assert config["HotelInfo"]["HotelCode"] == "HOTEL001"

        # Reservation structure
        assert "Reservations" in reservation
        assert len(reservation["Reservations"]) > 0
        res = reservation["Reservations"][0]
        assert "ResId" in res
        assert "Guests" in res
        assert "Prices" in res

        # Inventory structure
        assert "hotelCode" in inventory
        assert "roomInventories" in inventory
        room = inventory["roomInventories"][0]
        assert "dailyInventories" in room

        # Revenue structure
        assert "hotelCode" in revenue
        assert "transactions" in revenue
        assert len(revenue["transactions"]) > 0

        logger.info("Fixture structure validation passed!")

    def test_room_inventory_structure(self):
        """Verify room inventory data has correct structure.

        Validates the roomInventories array with all required fields:
        - date: string with ISO format (YYYY-MM-DD)
        - inventory: integer (rooms quantity)
        - inventoryOOI: integer (0 or 1, Out of Inventory flag)
        - inventoryOOO: integer (0 or 1, Out of Occupation flag)
        - roomCode: string (room code matching segments and reservations)
        """
        inventory = load_fixture("host_pms_api/inventory_response.json")

        # Validate basic structure
        assert "hotelCode" in inventory, "Missing hotelCode in inventory"
        assert "roomInventories" in inventory, "Missing roomInventories in inventory"
        assert len(inventory["roomInventories"]) > 0, "No room inventories found"

        # Validate each room inventory entry
        for room_inv in inventory["roomInventories"]:
            assert "roomCode" in room_inv, f"Missing roomCode in {room_inv}"
            assert isinstance(room_inv["roomCode"], str), "roomCode must be string"

            # Validate dailyInventories array
            assert "dailyInventories" in room_inv, f"Missing dailyInventories in room {room_inv['roomCode']}"
            assert len(room_inv["dailyInventories"]) > 0, f"No daily inventories for room {room_inv['roomCode']}"

            # Validate each daily inventory entry
            for daily_inv in room_inv["dailyInventories"]:
                # Validate required fields
                assert "date" in daily_inv, f"Missing date in daily inventory for {room_inv['roomCode']}"
                assert "inventory" in daily_inv, f"Missing inventory in daily inventory for {room_inv['roomCode']}"
                assert "inventoryOOI" in daily_inv, f"Missing inventoryOOI in daily inventory for {room_inv['roomCode']}"
                assert "inventoryOOO" in daily_inv, f"Missing inventoryOOO in daily inventory for {room_inv['roomCode']}"

                # Validate field types
                assert isinstance(daily_inv["date"], str), f"date must be string for {room_inv['roomCode']}"
                assert isinstance(daily_inv["inventory"], (int, float)), f"inventory must be numeric for {room_inv['roomCode']}"
                assert isinstance(daily_inv["inventoryOOI"], int), f"inventoryOOI must be integer for {room_inv['roomCode']}"
                assert isinstance(daily_inv["inventoryOOO"], int), f"inventoryOOO must be integer for {room_inv['roomCode']}"

                # Validate date format (should be ISO format YYYY-MM-DD)
                assert len(daily_inv["date"]) >= 10, f"date should be in ISO format (YYYY-MM-DD) for {room_inv['roomCode']}"
                assert "-" in daily_inv["date"], f"date should contain hyphens (YYYY-MM-DD) for {room_inv['roomCode']}"

                # Validate OOI and OOO are boolean-like (0 or 1)
                assert daily_inv["inventoryOOI"] in (0, 1), f"inventoryOOI must be 0 or 1 for {room_inv['roomCode']}"
                assert daily_inv["inventoryOOO"] in (0, 1), f"inventoryOOO must be 0 or 1 for {room_inv['roomCode']}"

                # Validate inventory is non-negative
                assert daily_inv["inventory"] >= 0, f"inventory must be non-negative for {room_inv['roomCode']}"

        logger.info("Room inventory structure validation passed!")
        for room_inv in inventory["roomInventories"]:
            first_inv = room_inv["dailyInventories"][0]
            logger.info(f"Room {room_inv['roomCode']} inventory",
                        daily_inventories_count=len(room_inv['dailyInventories']),
                        date=first_inv['date'],
                        inventory=first_inv['inventory'],
                        ooi=first_inv['inventoryOOI'],
                        ooo=first_inv['inventoryOOO'])

    def test_transformation_logic_with_fixtures(self):
        """Test transformation logic independently using fixtures."""
        from src.transformers.reservation_transformer import ReservationTransformer

        reservation_data = load_fixture("host_pms_api/reservation_response.json")
        expected_output = load_fixture("transformed/reservation_climber_format.json")

        # Get first reservation from fixture
        res = reservation_data["Reservations"][0]

        # Transform it
        transformer = ReservationTransformer()
        result = transformer.transform(res)

        # Verify key fields match expected output
        assert result.reservation_id == expected_output["reservationId"]
        assert result.pax == expected_output["pax"]
        assert result.revenue_room == expected_output["revenueRoom"]
        assert result.revenue_fb == expected_output["revenueFb"]

        logger.info("Transformation logic test passed!",
                    reservation_id=result.reservation_id,
                    revenue_room=result.revenue_room,
                    revenue_fb=result.revenue_fb)

    def test_all_fixtures_are_valid_json(self):
        """Verify all fixture files are valid JSON."""
        fixtures = [
            "host_pms_api/config_response.json",
            "host_pms_api/reservation_response.json",
            "host_pms_api/inventory_response.json",
            "host_pms_api/revenue_response.json",
            "transformed/reservation_climber_format.json",
            "transformed/inventory_climber_format.json",
            "edge_cases/cancelled_reservation.json",
        ]

        for fixture_path in fixtures:
            data = load_fixture(fixture_path)
            assert data is not None
            assert isinstance(data, (dict, list))
            logger.debug(f"Valid fixture: {fixture_path}")

        logger.info(f"All {len(fixtures)} fixtures are valid JSON!")


class TestLocalDevelopmentFlow:
    """Test scenarios for local development without AWS infrastructure."""

    def test_load_and_process_fixture_locally(self):
        """
        Demonstrates how to load fixture and process it locally
        for development/debugging.
        """
        from src.transformers.config_transformer import ConfigTransformer

        # Step 1: Load fixture
        config_data = load_fixture("host_pms_api/config_response.json")
        logger.info("Loaded config fixture from tests/fixtures/",
                    hotel_name=config_data['HotelInfo']['HotelName'])

        # Step 2: Transform it
        config_output, segments = ConfigTransformer.transform(config_data)
        logger.info("Transformed config",
                    room_count=config_output.room_count,
                    room_codes=[r.code for r in config_output.rooms])

        # Step 3: Transform segments
        logger.info("Extracted segments",
                    agencies=len(segments.agencies),
                    channels=len(segments.channels),
                    rates=len(segments.rates))

        # Step 4: Save outputs locally for inspection
        output_dir = Path(__file__).parent / "test_outputs"
        output_dir.mkdir(exist_ok=True)

        with open(output_dir / "transformed_config.json", "w") as f:
            json.dump(config_output.model_dump(), f, indent=2, default=str)

        logger.info("Saved transformed output to tests/test_outputs/transformed_config.json")

    def test_quick_manual_flow(self):
        """
        Quick test to manually process fixture data without async/mocking.
        Great for debugging transformation logic.
        """
        # Load all fixtures
        logger.info("Manual ETL Flow Test starting")

        config = load_fixture("host_pms_api/config_response.json")
        reservations = load_fixture("host_pms_api/reservation_response.json")
        inventory = load_fixture("host_pms_api/inventory_response.json")

        # Process each step
        logger.info("Processing hotel configuration",
                    hotel_code=config['HotelInfo']['HotelCode'],
                    hotel_name=config['HotelInfo']['HotelName'])

        # Count items
        room_types = len([
            i for i in config["ConfigInfo"]
            if i["ConfigType"] == "CATEGORY"
        ])
        logger.info("Hotel configuration summary", room_types=room_types)

        # Process reservations
        res_count = len(reservations["Reservations"])
        logger.info(f"Processing {res_count} reservation(s)")

        for res in reservations["Reservations"]:
            logger.debug(f"Reservation processing",
                        reservation_id=res['ResId'],
                        guests=res['Pax'])

        # Process inventory
        room_inv_count = len(inventory["roomInventories"])
        logger.info("Room inventory entries", count=room_inv_count)

        logger.info("Manual flow test completed!")


# Example: How to run this in your code
def example_usage():
    """Example of how to use fixtures in your main code."""

    print("""
    # OPTION 1: Run pytest tests
    pytest tests/test_etl_flow_with_fixtures.py -v -s

    # OPTION 2: Use fixtures in your main orchestration
    from pathlib import Path
    import json

    def load_fixture(filename):
        fixtures_dir = Path(__file__).parent / "fixtures"
        with open(fixtures_dir / filename) as f:
            return json.load(f)

    # In your main.py or run.py
    config_data = load_fixture("host_pms_api/config_response.json")
    result = orchestrator.process_hotel("HOTEL001", mock_data=config_data)

    # OPTION 3: Use conftest.py fixtures directly
    def test_something(host_config_response):
        # host_config_response is automatically injected
        assert host_config_response["HotelInfo"]["HotelCode"] == "HOTEL001"
    """)


if __name__ == "__main__":
    # For manual testing without pytest
    test = TestLocalDevelopmentFlow()
    test.test_load_and_process_fixture_locally()
    test.test_quick_manual_flow()
