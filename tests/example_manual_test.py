#!/usr/bin/env python3
"""
Manual example: Load fixtures and test your code directly.

This is the simplest way to test - no pytest, no mocking required.
Just load JSON fixtures and run your transformers/orchestration.

Usage:
    python3 example_manual_test.py
"""

import json
import asyncio
from pathlib import Path
from datetime import datetime

# Setup
FIXTURES_DIR = Path(__file__).parent / "tests" / "fixtures"


def load_fixture(filename):
    """Load a JSON fixture file."""
    path = FIXTURES_DIR / filename
    with open(path) as f:
        return json.load(f)


def example_1_load_and_inspect_fixtures():
    """Example 1: Load fixtures and inspect the data."""
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Load and Inspect Fixtures")
    print("=" * 70)

    # Load fixtures
    print("\nLoading fixture data...")
    config = load_fixture("host_pms_api/config_response.json")
    reservations = load_fixture("host_pms_api/reservation_response.json")
    inventory = load_fixture("host_pms_api/inventory_response.json")

    # Inspect config
    print("\n>>> Hotel Configuration:")
    print(f"    Hotel: {config['HotelInfo']['HotelName']}")
    print(f"    Code: {config['HotelInfo']['HotelCode']}")
    print(f"    Email: {config['HotelInfo']['HotelEmail']}")

    room_types = [i for i in config["ConfigInfo"] if i["ConfigType"] == "CATEGORY"]
    print(f"\n    Room Types ({len(room_types)}):")
    for room in room_types:
        print(f"      - {room['Code']:12} {room['Description']:25} (Qty: {room['Inventory']})")

    # Inspect reservation
    print("\n>>> Reservations:")
    res = reservations["Reservations"][0]
    print(f"    Reservation ID: {res['ResId']}")
    print(f"    Check-in: {res['CheckIn'][:10]}")
    print(f"    Check-out: {res['CheckOut'][:10]}")
    print(f"    Status: {res['ResStatus']}")
    print(f"    Guests: {res['Pax']}")

    print(f"\n    Guest Details ({len(res['Guests'])}):")
    for guest in res["Guests"]:
        print(f"      - {guest['NameFormatted']:20} ({guest['CountryIsoCode']})")

    print(f"\n    Revenue Items ({len(res['Prices'])}):")
    for price in res["Prices"]:
        sg = "Room" if price["SalesGroup"] == 0 else "F&B"
        print(f"      - {sg:5} {price['Charge']:8} ${price['Amount']:8.2f} ({price['Date']})")

    # Inspect inventory
    print("\n>>> Room Inventory:")
    for room_inv in inventory["roomInventories"]:
        print(f"\n    {room_inv['roomCode']}:")
        for daily in room_inv["dailyInventories"]:
            print(f"      {daily['date']} - Inv: {daily['inventory']}, "
                  f"OOI: {daily['inventoryOOI']}, OOO: {daily['inventoryOOO']}")


def example_2_transform_data():
    """Example 2: Transform fixture data using your transformers."""
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Transform Data")
    print("=" * 70)

    try:
        from src.transformers.config_transformer import ConfigTransformer
        from src.transformers.reservation_transformer import ReservationTransformer

        # Load fixtures
        print("\nLoading fixtures...")
        config_data = load_fixture("host_pms_api/config_response.json")
        reservation_data = load_fixture("host_pms_api/reservation_response.json")

        # Transform config
        print("\n>>> Transforming Configuration...")
        config_output, segments = ConfigTransformer.transform(config_data)

        print(f"    Hotel Config:")
        print(f"      Hotel Code: {config_data['HotelInfo']['HotelCode']}")
        print(f"      Hotel Name: {config_data['HotelInfo']['HotelName']}")
        print(f"      Room Count: {config_output.room_count}")
        print(f"      Rooms: {[r.code for r in config_output.rooms]}")

        print(f"\n    Segments Extracted:")
        print(f"      Agencies: {len(segments.agencies)}")
        print(f"      Channels: {len(segments.channels)}")
        print(f"      Rates: {len(segments.rates)}")
        print(f"      Rooms: {len(segments.rooms)}")
        print(f"      Segments: {len(segments.segments)}")
        print(f"      Sub-segments: {len(segments.sub_segments)}")

        # Transform reservation
        print("\n>>> Transforming Reservation...")
        res = reservation_data["Reservations"][0]
        transformer = ReservationTransformer()
        res_output = transformer.transform(res)

        print(f"    Input:")
        print(f"      ResId: {res['ResId']}")
        print(f"      Guests: {res['Pax']}")
        print(f"      Status: {res['ResStatus']}")

        print(f"\n    Output (Climber Format):")
        print(f"      reservationId: {res_output.reservationId}")
        print(f"      pax: {res_output.pax}")
        print(f"      status: {res_output.status}")
        print(f"      revenueRoom: ${res_output.revenueRoom}")
        print(f"      revenueFb: ${res_output.revenueFb}")
        print(f"      roomCode: {res_output.roomCode}")
        print(f"      agencyCode: {res_output.agencyCode}")

    except Exception as e:
        print(f"\n    ✗ Error: {e}")
        print("    (Make sure dependencies are installed: pip install pydantic structlog)")


def example_3_save_transformed_output():
    """Example 3: Save transformed output for inspection."""
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Save Transformed Output")
    print("=" * 70)

    try:
        from src.transformers.config_transformer import ConfigTransformer

        config_data = load_fixture("host_pms_api/config_response.json")

        print("\nTransforming config...")
        config_output, segments = ConfigTransformer.transform(config_data)

        # Create output directory
        output_dir = Path(__file__).parent / "tests" / "test_outputs"
        output_dir.mkdir(exist_ok=True)

        # Save config
        config_file = output_dir / "example_config_output.json"
        with open(config_file, "w") as f:
            json.dump(config_output.model_dump(), f, indent=2, default=str)
        print(f"  ✓ Saved: {config_file}")

        # Save segments
        segments_file = output_dir / "example_segments_output.json"
        with open(segments_file, "w") as f:
            json.dump(segments.model_dump(by_alias=True), f, indent=2, default=str)
        print(f"  ✓ Saved: {segments_file}")

        print(f"\nYou can now inspect these files:")
        print(f"  cat tests/test_outputs/example_config_output.json")
        print(f"  cat tests/test_outputs/example_segments_output.json")

    except Exception as e:
        print(f"\n  ✗ Error: {e}")


def example_4_mock_and_test_orchestration():
    """Example 4: Mock API and test orchestration."""
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Mock and Test Orchestration")
    print("=" * 70)

    try:
        from unittest.mock import AsyncMock, MagicMock
        from src.services.orchestration_service import HostPMSConnectorOrchestrator

        print("\nSetting up mock orchestration test...")

        # Load fixtures
        config_data = load_fixture("host_pms_api/config_response.json")
        reservation_data = load_fixture("host_pms_api/reservation_response.json")

        # Create orchestrator
        orchestrator = HostPMSConnectorOrchestrator()

        # Mock Host API
        orchestrator.host_api_client = AsyncMock()
        orchestrator.host_api_client.get_hotel_config = AsyncMock(return_value=config_data)
        orchestrator.host_api_client.get_reservations = AsyncMock(return_value=reservation_data)

        # Mock ESB client
        orchestrator.esb_client = AsyncMock()
        orchestrator.esb_client.get_hotel_parameters = AsyncMock(
            return_value={"lastImportDate": "2024-10-01"}
        )
        orchestrator.esb_client.register_file = AsyncMock(return_value=None)
        orchestrator.esb_client.update_import_date = AsyncMock(return_value=None)

        # Mock S3
        orchestrator.s3_manager = MagicMock()
        orchestrator.s3_manager.upload_raw = MagicMock(
            return_value={"key": "raw/HOTEL001/config/2024-10-26.json",
                         "url": "s3://bucket/raw/..."}
        )
        orchestrator.s3_manager.upload_processed = MagicMock(
            return_value={"key": "processed/HOTEL001/config/2024-10-26.json",
                         "url": "s3://bucket/processed/..."}
        )

        # Mock SQS
        orchestrator.sqs_manager = MagicMock()
        orchestrator.sqs_manager.send_message = MagicMock(
            return_value={"message_id": f"msg-{datetime.now().timestamp()}"}
        )

        print("  ✓ Mocks configured")

        # Run orchestration
        print("\n  Running orchestration...")

        async def run_test():
            result = await orchestrator.process_hotel("HOTEL001")
            return result

        result = asyncio.run(run_test())

        # Display results
        print(f"\n>>> Results:")
        print(f"    Success: {result['success']}")
        print(f"    Hotel Code: {result['hotel_code']}")

        if result["config"]:
            print(f"\n    Config:")
            print(f"      Room Count: {result['config']['room_count']}")
            print(f"      Key: {result['config']['processed_key']}")

        if result["reservations"]:
            print(f"\n    Reservations:")
            print(f"      Record Count: {result['reservations']['record_count']}")
            print(f"      Key: {result['reservations']['processed_key']}")

        if result["sqs_messages"]:
            print(f"\n    SQS Messages ({len(result['sqs_messages'])}):")
            for msg in result["sqs_messages"]:
                print(f"      - {msg['file_type']:15} {msg.get('sqs_message_id', 'pending')}")

        if result["errors"]:
            print(f"\n    Errors ({len(result['errors'])}):")
            for err in result["errors"]:
                print(f"      - {err}")

    except Exception as e:
        print(f"\n  ✗ Error: {e}")
        import traceback
        traceback.print_exc()


def example_5_verify_expected_output():
    """Example 5: Compare with expected output."""
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Verify Expected Output")
    print("=" * 70)

    try:
        from src.transformers.reservation_transformer import ReservationTransformer

        # Load fixture and expected output
        print("\nLoading fixture and expected output...")
        reservation_data = load_fixture("host_pms_api/reservation_response.json")
        expected = load_fixture("transformed/reservation_climber_format.json")

        # Transform
        res = reservation_data["Reservations"][0]
        transformer = ReservationTransformer()
        result = transformer.transform(res)

        # Compare
        print("\n>>> Verification:")
        fields_to_check = [
            ("reservationId", "reservationId"),
            ("pax", "pax"),
            ("status", "status"),
            ("revenueRoom", "revenueRoom"),
            ("revenueFb", "revenueFb"),
            ("roomCode", "roomCode"),
            ("agencyCode", "agencyCode"),
        ]

        all_match = True
        for attr, key in fields_to_check:
            actual = getattr(result, attr)
            expected_val = expected.get(key)
            match = actual == expected_val
            symbol = "✓" if match else "✗"
            print(f"    {symbol} {attr:15} {actual!r:20} == {expected_val!r}")
            if not match:
                all_match = False

        if all_match:
            print("\n  ✓ All fields match expected output!")
        else:
            print("\n  ✗ Some fields don't match - check transformation logic")

    except Exception as e:
        print(f"\n  ✗ Error: {e}")


def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " Manual Testing Examples - Using Fixtures".center(68) + "║")
    print("╚" + "=" * 68 + "╝")

    examples = [
        ("Load and Inspect Fixtures", example_1_load_and_inspect_fixtures),
        ("Transform Data", example_2_transform_data),
        ("Save Transformed Output", example_3_save_transformed_output),
        ("Mock and Test Orchestration", example_4_mock_and_test_orchestration),
        ("Verify Expected Output", example_5_verify_expected_output),
    ]

    for name, func in examples:
        try:
            func()
        except Exception as e:
            print(f"\n✗ Error in {name}: {e}")

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)
    print("""
Next steps:
  1. Check tests/test_outputs/ for generated files
  2. Inspect the transformed JSON files
  3. Run: pytest tests/test_with_fixtures.py -v
  4. Run: pytest tests/test_etl_flow_with_fixtures.py -v

For more info, read: QUICK_START.md or TESTING_GUIDE.md
""")


if __name__ == "__main__":
    main()
