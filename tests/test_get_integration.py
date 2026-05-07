#!/usr/bin/env python3
"""Test script for getIntegration endpoint and per-hotel credentials.

This script tests the new implementation that fetches hotel credentials
from the ESB getIntegration endpoint and uses per-hotel subscription keys.

Usage:
    # Test fetching integration list
    python tests/test_get_integration.py --test-fetch

    # Test processing single hotel credentials
    python tests/test_get_integration.py --test-single PTFNCTVB

    # Test processing all hotels (dry run - just list hotels)
    python tests/test_get_integration.py --test-list

    # Test invalid subscription key handling (abort processing)
    python tests/test_get_integration.py --test-invalid

    # Full test with real API calls (mock S3/SQS)
    python tests/test_get_integration.py --test-full PTFNCTVB

    # Run all basic tests
    python tests/test_get_integration.py
"""

import asyncio
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.esb_client import ClimberESBClient
from src.clients.host_api_client import HostPMSAPIClient
from src.config import configure_logging, get_logger
from src.services.orchestration_service import HostPMSConnectorOrchestrator

logger = get_logger(__name__)


async def test_fetch_integration():
    """Test 1: Fetch hotel list from getIntegration endpoint."""
    print("\n" + "=" * 70)
    print("TEST 1: Fetch Hotels from getIntegration Endpoint")
    print("=" * 70)

    try:
        esb_client = ClimberESBClient()

        print("\nFetching hotels from getIntegration endpoint...")
        hotels = await esb_client.get_integration("BITZ")

        print(f"\n✓ Successfully fetched {len(hotels)} hotels\n")

        # Display hotel list
        print("Hotel List:")
        print("-" * 70)
        print(f"{'Code':<15} {'Hotel ID':<12} {'Integration':<12} {'Has Credentials':<15}")
        print("-" * 70)

        for hotel in hotels:
            code = hotel.get("code", "N/A")
            hotel_id = hotel.get("hotel_id", "N/A")
            integration = hotel.get("integration_type", "N/A")
            has_creds = "✓" if hotel.get("auth_password") else "✗"

            print(f"{code:<15} {hotel_id:<12} {integration:<12} {has_creds:<15}")

        print("-" * 70)

        # Show first hotel details
        if hotels:
            print("\nFirst Hotel Details:")
            print(json.dumps(hotels[0], indent=2))

        return True

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_single_hotel_credentials(hotel_code: str):
    """Test 2: Create HostPMSAPIClient with hotel-specific credentials."""
    print("\n" + "=" * 70)
    print(f"TEST 2: Create API Client with Hotel-Specific Credentials ({hotel_code})")
    print("=" * 70)

    try:
        esb_client = ClimberESBClient()

        # Fetch hotels from getIntegration
        print(f"\n1. Fetching hotels from getIntegration...")
        hotels = await esb_client.get_integration("BITZ")

        # Find specific hotel
        hotel_data = next((h for h in hotels if h.get("code") == hotel_code), None)

        if not hotel_data:
            print(f"✗ Hotel {hotel_code} not found in integration endpoint")
            print(f"  Available hotels: {[h.get('code') for h in hotels]}")
            return False

        print(f"✓ Found hotel: {hotel_code}")

        # Extract credentials
        subscription_key = hotel_data.get("auth_password")

        if not subscription_key:
            print(f"✗ No auth_password found for hotel {hotel_code}")
            return False

        print(f"✓ Retrieved subscription key: {subscription_key[:10]}...")

        # Create API client with hotel-specific credentials
        print(f"\n2. Creating HostPMSAPIClient with hotel-specific credentials...")
        host_api_client = HostPMSAPIClient(subscription_key=subscription_key)

        print(f"✓ Created client:")
        print(f"  - Base URL: {host_api_client.base_url}")
        print(f"  - Subscription Key: {host_api_client.subscription_key[:10]}...")

        # Test API call (optional - requires valid credentials)
        print(f"\n3. Testing API call to Host PMS...")
        try:
            config = host_api_client.get_hotel_config(hotel_code)
            hotel_name = config.get("HotelInfo", {}).get("HotelName", "N/A")
            print(f"✓ Successfully fetched config for: {hotel_name}")
            print(f"  Hotel Code: {hotel_code}")
        except Exception as e:
            print(f"✗ API call failed: {e}")
            print(f"  (This might be expected if credentials are invalid)")

        return True

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_list_hotels():
    """Test 3: List all hotels without processing."""
    print("\n" + "=" * 70)
    print("TEST 3: List All Hotels (Dry Run)")
    print("=" * 70)

    try:
        orchestrator = HostPMSConnectorOrchestrator()

        print("\nFetching hotels from getIntegration endpoint...")
        hotels = await orchestrator.esb_client.get_integration("BITZ")

        print(f"\n✓ Found {len(hotels)} hotels to process\n")

        print("Hotels that would be processed:")
        print("-" * 70)
        print(f"{'#':<5} {'Code':<15} {'Hotel ID':<12} {'Status':<20}")
        print("-" * 70)

        for idx, hotel in enumerate(hotels, 1):
            code = hotel.get("code", "N/A")
            hotel_id = hotel.get("hotel_id", "N/A")
            has_creds = hotel.get("auth_password")
            status = "✓ Ready" if has_creds else "✗ Missing credentials"

            print(f"{idx:<5} {code:<15} {hotel_id:<12} {status:<20}")

        print("-" * 70)

        return True

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_invalid_subscription_key():
    """Test 4: Test invalid subscription key handling."""
    print("\n" + "=" * 70)
    print("TEST 4: Invalid Subscription Key Handling")
    print("=" * 70)

    try:
        from src.services.orchestration_service import HostPMSConnectorOrchestrator

        print("\nTesting with invalid subscription key...")

        # Create orchestrator
        orchestrator = HostPMSConnectorOrchestrator()

        # Create API client with invalid subscription key
        invalid_client = HostPMSAPIClient(subscription_key="INVALID_KEY_123")

        # Try to process hotel with invalid credentials
        result = await orchestrator.process_hotel("TEST_HOTEL", invalid_client)

        print("\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)

        print(f"\nSuccess: {result['success']}")
        print(f"Hotel Code: {result['hotel_code']}")

        if result.get("errors"):
            print(f"\nErrors ({len(result['errors'])}):")
            for error in result["errors"]:
                print(f"  - Step: {error.get('step', 'unknown')}")
                print(f"    Type: {error.get('error_type', 'N/A')}")
                print(f"    Message: {error.get('message', str(error))}")

        # Verify the error is an authentication error
        if result.get("errors"):
            is_auth_error = any(
                e.get("error_type") == "AUTHENTICATION_FAILED" for e in result["errors"]
            )
            if is_auth_error:
                print("\n✓ Authentication error properly detected and handled!")
                print("  Hotel was skipped before running the pipeline.")
                print("  In batch processing, next hotels would continue to be processed.")
                return True
            else:
                print("\n✗ Expected authentication error but got different error type")
                return False
        else:
            print("\n✗ Expected errors but none were returned")
            return False

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_process_single_hotel(hotel_code: str):
    """Test 5: Process single hotel using new process_single_hotel method."""
    print("\n" + "=" * 70)
    print(f"TEST 5: Process Single Hotel ({hotel_code})")
    print("=" * 70)

    try:
        # Use LocalTestOrchestrator with mock S3/SQS but real ESB
        from tests.local_test_orchestrator import LocalTestOrchestrator

        print(f"\nInitializing test orchestrator...")
        orchestrator = LocalTestOrchestrator(
            output_dir="./data_extracts", use_real_esb=True  # Use real ESB to test getIntegration
        )

        print(f"Processing hotel: {hotel_code}")
        print("(Using mock S3 and SQS - files will be saved locally)\n")

        # Process single hotel
        result = await orchestrator.process_single_hotel(hotel_code)

        # Display results
        print("\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)

        print(f"\nSuccess: {result['success']}")
        print(f"Hotel Code: {result['hotel_code']}")

        if result.get("errors"):
            print(f"\nErrors ({len(result['errors'])}):")
            for error in result["errors"]:
                print(f"  - [{error.get('step', 'unknown')}] {error.get('message', str(error))}")

        if result.get("stats"):
            print(f"\nStatistics:")
            for key, value in result["stats"].items():
                print(f"  - {key}: {value}")

        if result.get("s3_uploads"):
            print(f"\nS3 Uploads ({len(result['s3_uploads'])}):")
            for key, upload in result["s3_uploads"].items():
                print(f"  - {key}: {upload.get('url', 'N/A')}")

        # Check for output files
        from pathlib import Path

        output_dir = Path("./data_extracts")
        if output_dir.exists():
            hotel_dirs = list(output_dir.glob(f"{hotel_code}_*"))
            if hotel_dirs:
                print(f"\nLocal Files:")
                for hotel_dir in hotel_dirs:
                    files = list(hotel_dir.glob("*.json"))
                    print(f"  Directory: {hotel_dir}")
                    for file in files:
                        size_kb = file.stat().st_size / 1024
                        print(f"    - {file.name} ({size_kb:.1f} KB)")

        return result["success"]

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main test runner."""
    import argparse

    parser = argparse.ArgumentParser(description="Test getIntegration endpoint implementation")
    parser.add_argument("--test-fetch", action="store_true", help="Test fetching integration list")
    parser.add_argument("--test-single", metavar="HOTEL_CODE", help="Test single hotel credentials")
    parser.add_argument("--test-list", action="store_true", help="List all hotels (dry run)")
    parser.add_argument(
        "--test-invalid", action="store_true", help="Test invalid subscription key handling"
    )
    parser.add_argument("--test-full", metavar="HOTEL_CODE", help="Full test with real API calls")

    args = parser.parse_args()

    # Configure logging
    configure_logging()

    # Run tests
    if args.test_fetch:
        success = asyncio.run(test_fetch_integration())
        sys.exit(0 if success else 1)

    elif args.test_single:
        success = asyncio.run(test_single_hotel_credentials(args.test_single))
        sys.exit(0 if success else 1)

    elif args.test_list:
        success = asyncio.run(test_list_hotels())
        sys.exit(0 if success else 1)

    elif args.test_invalid:
        success = asyncio.run(test_invalid_subscription_key())
        sys.exit(0 if success else 1)

    elif args.test_full:
        success = asyncio.run(test_process_single_hotel(args.test_full))
        sys.exit(0 if success else 1)

    else:
        # Run all tests
        print("\n")
        print("╔" + "=" * 68 + "╗")
        print("║" + " Testing getIntegration Implementation".center(68) + "║")
        print("╚" + "=" * 68 + "╝")

        all_success = True

        # Test 1: Fetch integration
        if not asyncio.run(test_fetch_integration()):
            all_success = False

        # Test 2: List hotels
        if not asyncio.run(test_list_hotels()):
            all_success = False

        # Test 3: Invalid subscription key
        if not asyncio.run(test_invalid_subscription_key()):
            all_success = False

        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)

        if all_success:
            print("\n✓ All basic tests passed!")
            print("\nNext steps:")
            print(
                "  1. Test single hotel: python tests/test_get_integration.py --test-single PTFNCTVB"
            )
            print("  2. Full test: python tests/test_get_integration.py --test-full PTFNCTVB")
        else:
            print("\n✗ Some tests failed")

        sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
