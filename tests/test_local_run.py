"""Local test script for running the pipeline without AWS/Climber infrastructure.

This script uses the LocalTestOrchestrator to test the full pipeline locally:
- Fetches real data from Host PMS API OR reprocesses existing raw data
- Saves files to data_extracts directory instead of S3
- Logs ESB registrations instead of calling API (or uses real ESB if USE_REAL_ESB=true)
- Logs SQS messages instead of sending
- Optionally imports data to PostgreSQL database (if IMPORT_TO_DB=true)

Usage (Fetch from API):
    # Mock ESB (default - no real API calls)
    python -m tests.test_local_run

    # Real ESB with Redis + OAuth authentication
    USE_REAL_ESB=true python -m tests.test_local_run

    # With database import enabled
    IMPORT_TO_DB=true python -m tests.test_local_run

    # Combined with hotel code, database import, and custom output
    IMPORT_TO_DB=true USE_REAL_ESB=true HOTEL_CODE_S3=QUATRO_VIAS_SA OUTPUT_DIR=./my_output python -m tests.test_local_run

Usage (Reprocess existing data - no API calls):
    # Reprocess from existing directory (avoids redundant API calls)
    RAW_DATA_PATH=HOTEL_CODE_20250401_123456 python -m tests.test_local_run

    # Reprocess with database import
    RAW_DATA_PATH=data_extracts/HOTEL_CODE_20250401_123456 IMPORT_TO_DB=true python -m tests.test_local_run

Environment Variables:
    - RAW_DATA_PATH: Path to existing raw data directory for reprocessing (skips API calls)
    - USE_REAL_ESB: Set to 'true' to use real ESB client (default: false)
    - IMPORT_TO_DB: Set to 'true' to import data to PostgreSQL (default: false)
    - HOTEL_CODE_S3: Climber hotel code to process (if not set, processes all hotels)
    - OUTPUT_DIR: Directory to save files (default: ./data_extracts)
    - DATABASE_URL: PostgreSQL connection string (required if IMPORT_TO_DB=true)
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from .env file (for DATABASE_URL and other local testing vars)
from dotenv import load_dotenv

load_dotenv()

from src.clients.host_api_client import HostPMSAPIClient
from src.config import configure_logging, get_logger, settings
from src.models.host.config import HotelConfigResponse
from src.transformers.config_transformer import ConfigTransformer
from src.transformers.stat_daily_to_reservation_transformer import StatDailyToReservationTransformer
from tests.local_test_orchestrator import LocalTestOrchestrator

# Optional PostgreSQL imports (for local testing only)
try:
    from tests.db.postgres_importer import import_reservations_to_postgres
    from tests.db.stat_daily_importer import import_stat_daily_to_postgres
    from tests.db.stat_summary_importer import import_stat_summary_to_postgres

    DB_IMPORT_AVAILABLE = True
except ImportError:
    DB_IMPORT_AVAILABLE = False
    import_reservations_to_postgres = None
    import_stat_daily_to_postgres = None
    import_stat_summary_to_postgres = None

logger = get_logger(__name__)


def fetch_stat_summary(hotel_code: str, hotel_dir: Path) -> None:
    """Fetch StatSummary validation data from Host PMS API.

    Args:
        hotel_code: Hotel code to fetch data for
        hotel_dir: Directory to save the data

    Notes:
        If API rate limit (429) is hit, waits 60 seconds and retries
    """
    print(f"\n📊 Fetching StatSummary validation data...")

    # Calculate date range: 2 years in the past and 1 year ahead
    today = datetime.now().date()
    start_date = today - timedelta(days=730)  # 2 years back
    end_date = today + timedelta(days=365)  # 1 year ahead

    from_date_str = start_date.isoformat()
    to_date_str = end_date.isoformat()

    print(f"   📅 Date range: {from_date_str} to {to_date_str} (2 years past + 1 year ahead)")

    # Retry loop for rate limit handling
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            # Create API client and fetch StatSummary
            client = HostPMSAPIClient()
            stat_summary_response = client.get_stat_summary(
                from_date=from_date_str, to_date=to_date_str, hotel_code=hotel_code
            )

            # Save raw StatSummary data
            if stat_summary_response:
                # Use MockS3Manager naming convention
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                stat_summary_file = hotel_dir / f"raw_stat_summary-{timestamp}.json"
                with open(stat_summary_file, "w") as f:
                    json.dump(stat_summary_response, f, indent=2)
                print(
                    f"   ✅ StatSummary saved: {stat_summary_file.name} ({len(stat_summary_response)} records)"
                )
            else:
                print(f"   ⚠️  No StatSummary data returned from API")

            # Success - exit retry loop
            return

        except Exception as e:
            error_msg = str(e)

            # Check for API rate limit exceeded (429 status code)
            if (
                "429" in error_msg
                or "API calls quota exceeded" in error_msg
                or "quota exceeded" in error_msg.lower()
            ):
                retry_count += 1

                if retry_count < max_retries:
                    print(f"\n⚠️  API rate limit exceeded! (attempt {retry_count}/{max_retries})")
                    print(f"   Error: {error_msg}")
                    print(f"   The Host PMS API has a limit of 60 calls per second.")
                    print(f"   ⏳ Waiting 60 seconds before retrying...")

                    logger.warning(
                        "API rate limit exceeded - waiting before retry",
                        hotel_code=hotel_code,
                        error=error_msg,
                        status_code=429,
                        retry_attempt=retry_count,
                        wait_seconds=60,
                    )

                    # Wait 60 seconds before retry
                    time.sleep(60)
                    print(f"   🔄 Retrying StatSummary fetch...")
                else:
                    # Max retries reached
                    print(f"\n❌ ERROR: API rate limit exceeded after {max_retries} attempts!")
                    print(f"   Error: {error_msg}")
                    print(f"   Skipping StatSummary fetch for this hotel.")
                    logger.error(
                        "API rate limit exceeded - max retries reached, skipping",
                        hotel_code=hotel_code,
                        error=error_msg,
                        status_code=429,
                        max_retries=max_retries,
                    )
                    return  # Skip this hotel's StatSummary, continue processing
            else:
                # For other errors, log and continue
                print(f"   ❌ Error fetching StatSummary: {error_msg}")
                logger.error(
                    "Error fetching StatSummary",
                    hotel_code=hotel_code,
                    error=error_msg,
                    exc_info=True,
                )
                return  # Exit on non-rate-limit errors


def reprocess_from_raw_data(raw_data_path: str, output_dir: str, import_to_db: bool) -> int:
    """Reprocess existing raw data without hitting the API.

    Args:
        raw_data_path: Path to existing raw data directory
        output_dir: Base output directory for reprocessed data
        import_to_db: Whether to import to database

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    logger.info("Starting data reprocessing from raw files", raw_data_path=raw_data_path)

    # Resolve raw data directory
    raw_data_dir = Path(raw_data_path)
    if not raw_data_dir.is_absolute():
        raw_data_dir = Path(output_dir) / raw_data_path

    if not raw_data_dir.exists():
        print(f"❌ Error: Raw data directory not found: {raw_data_dir}")
        print(f"   Tried path: {raw_data_dir.resolve()}")
        return 1

    # Extract hotel code from directory name (e.g., HOTEL_CODE_20250401_123456 -> HOTEL_CODE)
    dir_name = raw_data_dir.name
    parts = dir_name.split("_")
    if len(parts) < 3:
        print(f"❌ Error: Invalid directory name format: {dir_name}")
        print(f"   Expected format: HOTEL_CODE_TIMESTAMP (e.g., HOTEL001_20250401_123456)")
        return 1

    hotel_code = parts[0]

    # Create new output directory for reprocessed data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hotel_dir = Path(output_dir) / f"{dir_name}_reprocess_{timestamp}"
    hotel_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Re-processing data",
        hotel_code=hotel_code,
        raw_dir=str(raw_data_dir),
        output_dir=str(hotel_dir),
    )
    print(f"\n🔄 Reprocessing data from: {raw_data_dir.name}")
    print(f"📁 Output directory: {hotel_dir}")

    try:
        # ==================== LOAD CONFIG ====================
        print(f"\n1️⃣  Loading hotel configuration...")
        config_files = list(raw_data_dir.glob("raw_hotel-configs_*.json"))
        if not config_files:
            print(f"   ❌ No config file found in {raw_data_dir}")
            return 1

        config_file = config_files[0]
        with open(config_file, "r") as f:
            config_response = json.load(f)
        print(f"   ✅ Loaded config from: {config_file.name}")

        # Extract hotel local time for transformations
        hotel_local_time = None
        try:
            config_model = (
                HotelConfigResponse(**config_response)
                if isinstance(config_response, dict)
                else config_response
            )
            hotel_local_time = config_model.hotel_info.local_time
            if hotel_local_time:
                logger.info(
                    "Hotel local time extracted",
                    hotel_code=hotel_code,
                    local_time=str(hotel_local_time),
                )
        except Exception as e:
            logger.warning(
                "Could not extract hotel local time", hotel_code=hotel_code, error=str(e)
            )

        # ==================== LOAD RAW STATDAILY DATA ====================
        print(f"\n2️⃣  Loading raw StatDaily data...")
        raw_statdaily_files = list(raw_data_dir.glob("raw_reservations_*.json"))
        if not raw_statdaily_files:
            print(f"   ❌ No raw StatDaily file found in {raw_data_dir}")
            return 1

        raw_statdaily_file = raw_statdaily_files[0]
        with open(raw_statdaily_file, "r") as f:
            statdaily_records = json.load(f)
        print(
            f"   ✅ Loaded {len(statdaily_records)} StatDaily records from: {raw_statdaily_file.name}"
        )

        # ==================== TRANSFORM STATDAILY TO RESERVATIONS ====================
        print(f"\n3️⃣  Transforming StatDaily to reservations...")
        try:
            reservation_collection = StatDailyToReservationTransformer.transform_batch(
                statdaily_records,
                hotel_code=hotel_code,
                hotel_local_time=hotel_local_time,
                config_response=config_response,
            )

            # Save transformed reservations
            timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_file = (
                hotel_dir / f"processed_reservations_reservations-{timestamp_suffix}.json"
            )
            with open(processed_file, "w") as f:
                json.dump(json.loads(reservation_collection.model_dump_json()), f, indent=2)

            print(f"   ✅ Transformed {len(reservation_collection.reservations)} reservations")
            print(f"   ✅ Saved to: {processed_file.name}")

            # Also save the raw StatDaily data to new directory for completeness
            raw_output_file = hotel_dir / f"raw_reservations_reservations-{timestamp_suffix}.json"
            with open(raw_output_file, "w") as f:
                json.dump(statdaily_records, f, indent=2)
            print(f"   ✅ Copied raw data to: {raw_output_file.name}")

        except Exception as e:
            print(f"   ❌ Error transforming StatDaily: {str(e)}")
            logger.error("Error transforming StatDaily", error=str(e), exc_info=True)
            return 1

        # ==================== COPY STAT SUMMARY (VALIDATION DATA) ====================
        print(f"\n4️⃣  Copying StatSummary validation data...")
        stat_summary_files = list(raw_data_dir.glob("*stat_summary*.json"))
        if stat_summary_files:
            stat_summary_file = stat_summary_files[0]
            # Copy to new directory
            stat_summary_output = hotel_dir / stat_summary_file.name
            with open(stat_summary_file, "r") as f:
                stat_summary_data = json.load(f)
            with open(stat_summary_output, "w") as f:
                json.dump(stat_summary_data, f, indent=2)
            print(
                f"   ✅ Copied StatSummary data: {stat_summary_file.name} ({len(stat_summary_data)} records)"
            )
        else:
            print(f"   ℹ️  No StatSummary file found in raw data directory")

        # ==================== IMPORT TO DATABASE ====================
        if import_to_db:
            import_to_database(hotel_dir)

        # ==================== SUMMARY ====================
        print(f"\n✨ Reprocessing complete!")
        print(f"📁 All files saved to: {hotel_dir.absolute()}")
        print("\n📋 Files created:")
        for file in sorted(hotel_dir.glob("*.json")):
            size_kb = file.stat().st_size / 1024
            print(f"   - {file.name} ({size_kb:.1f} KB)")

        return 0

    except Exception as e:
        print(f"\n❌ Fatal error during reprocessing: {str(e)}")
        logger.error("Fatal error during reprocessing", error=str(e), exc_info=True)
        return 1


def import_to_database(hotel_dir: Path) -> None:
    """Import data from hotel directory to PostgreSQL database.

    Args:
        hotel_dir: Path to hotel output directory containing JSON files
    """
    if not DB_IMPORT_AVAILABLE:
        print("\n5️⃣  PostgreSQL import skipped (db module not available)")
        return

    # Check if DATABASE_URL is configured
    database_url = os.environ.get("DATABASE_URL")
    db_name = os.environ.get("DB_NAME")

    if not database_url and not db_name:
        print("\n5️⃣  PostgreSQL import skipped (DATABASE_URL not configured)")
        print("   ℹ️  To enable database import, set DATABASE_URL environment variable:")
        print('   export DATABASE_URL="postgresql://user:pass@localhost:5432/database"')
        return

    print("\n5️⃣  Importing data to PostgreSQL...")
    try:
        # Import reservations - look for processed reservations from StatDaily
        processed_reservations_files = list(hotel_dir.glob("processed_reservations_*.json"))

        if processed_reservations_files:
            # Use the first file found (there should only be one per run)
            reservations_file = processed_reservations_files[0]
            import_reservations_to_postgres(
                json_file_path=str(reservations_file),
                table_name="reservations_from_statdaily",
                truncate=True,
            )
            print(
                f"   ✅ Reservations from StatDaily imported to PostgreSQL (table: reservations_from_statdaily)"
            )
        else:
            print(f"   ⚠️  No reservations file found, skipping import")

        # Import StatDaily data
        raw_reservations_files = list(hotel_dir.glob("raw_reservations_*.json"))
        if raw_reservations_files:
            # Use the first file found
            stat_daily_file = raw_reservations_files[0]
            import_stat_daily_to_postgres(
                json_file_path=str(stat_daily_file), table_name="stat_daily", truncate=True
            )
            print(f"   ✅ StatDaily data imported to PostgreSQL")
        else:
            print(f"   ℹ️  No StatDaily file found, skipping import")

        # Import StatSummary data (validation data)
        stat_summary_files = list(hotel_dir.glob("*stat_summary*.json"))
        if stat_summary_files:
            stat_summary_file = stat_summary_files[0]
            import_stat_summary_to_postgres(
                json_file_path=str(stat_summary_file), table_name="stat_summary", truncate=True
            )
            print(f"   ✅ StatSummary data imported to PostgreSQL (validation table)")
        else:
            print(f"   ℹ️  No StatSummary file found, skipping import")

    except Exception as e:
        print(f"   ❌ Error importing to PostgreSQL: {str(e)}")
        print(f"   ℹ️  Database import failed, but data was successfully saved to files")


async def main() -> int:
    """Run the local test pipeline.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Get configuration from environment
    output_dir = os.getenv("OUTPUT_DIR", "./data_extracts")
    use_real_esb = os.getenv("USE_REAL_ESB", "false").lower() == "true"
    import_to_db = os.getenv("IMPORT_TO_DB", "false").lower() == "true"
    raw_data_path = os.getenv("RAW_DATA_PATH")

    # If RAW_DATA_PATH is set, skip orchestrator and reprocess existing data
    if raw_data_path:
        logger.info(
            "Reprocessing mode enabled",
            raw_data_path=raw_data_path,
            output_dir=output_dir,
            import_to_db=import_to_db,
        )
        return reprocess_from_raw_data(raw_data_path, output_dir, import_to_db)

    # Normal mode: fetch from API using orchestrator
    logger.info(
        "Starting Local Test Pipeline",
        environment=settings.environment,
        output_dir=output_dir,
        use_real_esb=use_real_esb,
        import_to_db=import_to_db,
    )

    try:
        # Create orchestrator with custom settings
        orchestrator = LocalTestOrchestrator(
            output_dir=output_dir,
            use_real_esb=use_real_esb,
        )

        # Check if specific hotel code is configured
        hotel_code = (settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or "").strip()

        if hotel_code:
            # Single hotel mode
            logger.info(
                "Processing single hotel (local test)",
                hotel_code=hotel_code,
                output_dir=output_dir,
            )
            result = await orchestrator.process_hotel(hotel_code)

            logger.info(
                "Local test pipeline complete",
                hotel_code=hotel_code,
                success=result["success"],
            )

            # Print results
            print("\n" + "=" * 80)
            print("LOCAL TEST RESULTS (Single Hotel)")
            print("=" * 80)
            print(json.dumps(result, indent=2, default=str))
            print("=" * 80)
            print(f"\nFiles saved to: {Path(output_dir).absolute()}")
            print("=" * 80 + "\n")

            # Fetch StatSummary validation data
            hotel_dir = Path(output_dir) / f"{hotel_code}_{orchestrator.s3_manager.timestamp}"
            fetch_stat_summary(hotel_code, hotel_dir)

            # Import to database if enabled
            if import_to_db:
                import_to_database(hotel_dir)

            return 0 if result["success"] else 1

        else:
            # Multi-hotel mode
            logger.info(
                "Processing all hotels (local test)",
                output_dir=output_dir,
            )
            results = await orchestrator.process_all_hotels()

            logger.info(
                "Local test pipeline complete",
                total_hotels=results["total_hotels"],
                successful_hotels=results["successful_hotels"],
                failed_hotels=results["failed_hotels"],
            )

            # Print results
            print("\n" + "=" * 80)
            print("LOCAL TEST RESULTS (All Hotels)")
            print("=" * 80)
            print(json.dumps(results, indent=2, default=str))
            print("=" * 80)
            print(f"\nFiles saved to: {Path(output_dir).absolute()}")
            print("=" * 80 + "\n")

            # Fetch StatSummary for all successfully processed hotels
            if results["successful_hotels"] > 0:
                output_path = Path(output_dir)
                timestamp_pattern = orchestrator.s3_manager.timestamp
                for hotel_dir in output_path.glob(f"*_{timestamp_pattern}"):
                    if hotel_dir.is_dir():
                        # Extract hotel code from directory name (e.g., HOTEL_CODE_20250401_123456)
                        dir_name = hotel_dir.name
                        hotel_code = dir_name.split("_")[0]
                        fetch_stat_summary(hotel_code, hotel_dir)

            # Import to database if enabled (process all hotel directories)
            if import_to_db and results["successful_hotels"] > 0:
                print("\n📊 Processing database imports for all hotels...")
                output_path = Path(output_dir)
                # Find all hotel directories created in this run
                timestamp_pattern = orchestrator.s3_manager.timestamp
                for hotel_dir in output_path.glob(f"*_{timestamp_pattern}"):
                    if hotel_dir.is_dir():
                        print(f"\n   Hotel: {hotel_dir.name}")
                        import_to_database(hotel_dir)

            if results["successful_hotels"] > 0:
                return 0
            elif results["failed_hotels"] == 0:
                logger.warning("No hotels were processed")
                return 1
            else:
                logger.error("All hotels failed to process")
                return 1

    except Exception as e:
        logger.error(
            "Fatal error in local test pipeline",
            error=str(e),
            exc_info=True,
        )
        print(f"\nERROR: {str(e)}\n")
        return 1


def run_sync() -> int:
    """Run the async main function synchronously.

    Returns:
        Exit code from main()
    """
    return asyncio.run(main())


if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Check environment variables
    use_real_esb = os.getenv("USE_REAL_ESB", "false").lower() == "true"
    import_to_db = os.getenv("IMPORT_TO_DB", "false").lower() == "true"
    raw_data_path = os.getenv("RAW_DATA_PATH")

    # Print banner
    print("\n" + "=" * 80)
    print("HOST PMS CONNECTOR - LOCAL TEST MODE")
    print("=" * 80)

    # Show different banner based on mode
    if raw_data_path:
        print("Mode: REPROCESSING (no API calls - transforms existing raw data)")
        print(f"Raw Data Path: {raw_data_path}")
        print("=" * 80)
    else:
        print("Mode: FETCH FROM API (pulls fresh data from Host PMS)")
        print("Files will be saved locally for inspection (no S3/SQS)")
        print("=" * 80)

        if use_real_esb:
            print("ESB Mode: REAL (Redis + OAuth authentication)")
            print(f"ESB URL: {os.getenv('ESB_BASE_URL', 'from settings')}")
            print(
                f"Redis: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}"
            )
        else:
            print("ESB Mode: MOCK (no real API calls)")

    # Database import status (applies to both modes)
    if import_to_db:
        database_url = os.getenv("DATABASE_URL")
        db_name = os.getenv("DB_NAME")
        if database_url or db_name:
            print("Database Import: ENABLED")
            print(f"Database: {db_name or 'from DATABASE_URL'}")
        else:
            print("Database Import: DISABLED (DATABASE_URL not configured)")
    else:
        print("Database Import: DISABLED")

    print("=" * 80 + "\n")

    # Run main function and exit with returned code
    sys.exit(run_sync())
