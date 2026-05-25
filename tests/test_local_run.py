"""Local test script for running the pipeline without AWS/Climber infrastructure.

This script uses the LocalTestOrchestrator to test the full pipeline locally:
- Fetches real data from Host PMS API OR reprocesses existing raw data
- Saves files to data_extracts directory instead of S3
- Logs ESB registrations instead of calling the ESB (no POST/PUT ever)
- Logs SQS messages instead of sending
- Imports data to the local PostgreSQL database (default ON — set IMPORT_TO_DB=false to skip)

This runner is sealed: S3, SQS, and ESB are always mocked. There is no env
var or flag that can switch it to a real ESB/S3/SQS client.

Usage (Fetch from API):
    # Default: fetches from API, writes JSON files, imports into the local DB
    python -m tests.test_local_run

    # Skip the DB import (still writes JSON files)
    IMPORT_TO_DB=false python -m tests.test_local_run

    # With hotel code and custom output
    HOTEL_CODE_S3=QUATRO_VIAS_SA OUTPUT_DIR=./my_output python -m tests.test_local_run

Usage (Reprocess existing data - no API calls):
    # Reprocess from existing directory (avoids redundant API calls)
    RAW_DATA_PATH=HOTEL_CODE_20250401_123456 python -m tests.test_local_run

Environment Variables:
    - RAW_DATA_PATH: Path to existing raw data directory for reprocessing (skips API calls)
    - IMPORT_TO_DB: 'true' (default) to import data to the local PostgreSQL after
                    the pipeline completes; set to 'false' to skip the DB import.
    - HOTEL_CODE_S3: Climber hotel code to process (if not set, processes all hotels)
    - OUTPUT_DIR: Directory to save files (default: ./data_extracts)
    - DATABASE_URL: PostgreSQL connection string (required if IMPORT_TO_DB=true)
    - FROM_DATE: Optional explicit start date (YYYY-MM-DD) for the import window.
                 When set, bypasses the is-first-import branching logic.
    - TO_DATE:   Optional explicit end date (YYYY-MM-DD) for the import window.
                 When set, bypasses the is-first-import branching logic.
    - IS_FIRST_IMPORT: Optional override ('true' / 'false') for the is_first_import
                 flag on the pipeline context. Useful when forcing a fresh-style
                 import while still pinning an explicit date range.
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

from src.config import configure_logging, get_logger, settings
from src.clients.host_api_client import HostPMSAPIClient
from src.aws.mock_s3_manager import MockS3Manager
from src.aws.mock_sqs_manager import MockSQSManager
from src.clients.mock_esb_client import MockClimberESBClient
from src.transformers.config_transformer import ConfigTransformer
from src.transformers.stat_daily_to_reservation_transformer import StatDailyToReservationTransformer
from src.models.host.config import HotelConfigResponse
from tests.local_test_orchestrator import LocalTestOrchestrator


def _assert_sealed(orchestrator: LocalTestOrchestrator) -> None:
    """Refuse to run if any external client is not a mock.

    Belt-and-suspenders: LocalTestOrchestrator is wired to mocks only, but this
    catches regressions if a future change re-introduces a real-client path.
    """
    if not isinstance(orchestrator.esb_client, MockClimberESBClient):
        raise RuntimeError(
            "Local test must use MockClimberESBClient — refusing to run against real ESB"
        )
    if not isinstance(orchestrator.s3_manager, MockS3Manager):
        raise RuntimeError("Local test must use MockS3Manager — refusing to run against real S3")
    if not isinstance(orchestrator.sqs_manager, MockSQSManager):
        raise RuntimeError("Local test must use MockSQSManager — refusing to run against real SQS")

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


def _parse_quota_window(error_msg: str) -> tuple[int | None, str | None]:
    """Extract (limit, window) from a Host PMS quota error like
    'maximum admitted 200 per Hour'. Returns (None, None) when not present.
    """
    import re

    match = re.search(r"maximum admitted (\d+) per (\w+)", error_msg, re.IGNORECASE)
    if not match:
        match = re.search(r"(\d+) per (\w+)", error_msg, re.IGNORECASE)
    if not match:
        return None, None
    return int(match.group(1)), match.group(2).lower()


def fetch_stat_summary(
    hotel_code: str,
    hotel_dir: Path,
    from_date_override: str | None = None,
    to_date_override: str | None = None,
    client: HostPMSAPIClient | None = None,
) -> None:
    """Fetch StatSummary validation data from Host PMS API.

    Args:
        hotel_code: Hotel code to fetch data for
        hotel_dir: Directory to save the data
        from_date_override: Optional explicit start date (YYYY-MM-DD).
        to_date_override: Optional explicit end date (YYYY-MM-DD).
        client: Optional preconfigured HostPMSAPIClient (with the per-hotel
            subscription key). If omitted, falls back to a default client built
            from .env — which has its own quota and may already be exhausted.

    Notes:
        On 429 we parse the actual quota window from the error message. Sub-hour
        windows (Second/Minute) get a short backoff and one retry. Hour-or-larger
        windows are not retried — waiting that long inside a test run is wrong;
        we skip and surface a clear message instead.
    """
    print(f"\n📊 Fetching StatSummary validation data...")

    today = datetime.now().date()
    if from_date_override:
        from_date_str = from_date_override
    else:
        from_date_str = (today - timedelta(days=730)).isoformat()  # 2 years back
    if to_date_override:
        to_date_str = to_date_override
    else:
        to_date_str = (today + timedelta(days=365)).isoformat()  # 1 year ahead

    if from_date_override or to_date_override:
        range_note = "FROM_DATE/TO_DATE override"
    else:
        range_note = "2 years past + 1 year ahead"
    print(f"   📅 Date range: {from_date_str} to {to_date_str} ({range_note})")

    if client is None:
        print(f"   ⚠️  No hotel-specific client provided — falling back to .env default subscription key")
    api_client = client or HostPMSAPIClient()

    max_short_retries = 2  # retries for sub-hour quotas only
    short_retry_count = 0

    while True:
        try:
            stat_summary_response = api_client.get_stat_summary(
                from_date=from_date_str,
                to_date=to_date_str,
                hotel_code=hotel_code,
            )

            if stat_summary_response:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                stat_summary_file = hotel_dir / f"raw_stat_summary-{timestamp}.json"
                with open(stat_summary_file, "w") as f:
                    json.dump(stat_summary_response, f, indent=2)
                print(f"   ✅ StatSummary saved: {stat_summary_file.name} ({len(stat_summary_response)} records)")
            else:
                print(f"   ⚠️  No StatSummary data returned from API")
            return

        except Exception as e:
            error_msg = str(e)
            is_rate_limit = (
                "429" in error_msg
                or "quota exceeded" in error_msg.lower()
            )
            if not is_rate_limit:
                print(f"   ❌ Error fetching StatSummary: {error_msg}")
                logger.error(
                    "Error fetching StatSummary",
                    hotel_code=hotel_code,
                    error=error_msg,
                    exc_info=True,
                )
                return

            limit, window = _parse_quota_window(error_msg)
            window_norm = (window or "").lower()

            if window_norm in ("hour", "day"):
                # Waiting hours inside a test run is wrong. Fail fast with a
                # clear explanation so the user knows what to do.
                print(f"\n❌ Host PMS hourly/daily quota exhausted on this subscription key — skipping StatSummary.")
                print(f"   Error: {error_msg}")
                print(f"   Parsed quota: {limit} per {window}.")
                print(f"   The pipeline's StatDaily fan-out can consume this quota when the same key is shared with the .env default.")
                print(f"   Resolution: wait for the quota window to reset, or use a per-hotel subscription key with sufficient budget.")
                logger.error(
                    "StatSummary skipped: quota window too long to wait inside test run",
                    hotel_code=hotel_code,
                    error=error_msg,
                    quota_limit=limit,
                    quota_window=window,
                )
                return

            # Sub-hour windows (Second / Minute) — short backoff and one retry.
            short_retry_count += 1
            if short_retry_count > max_short_retries:
                print(f"\n❌ Sub-hour rate limit persisted after {max_short_retries} retries — skipping StatSummary.")
                print(f"   Error: {error_msg}")
                logger.error(
                    "StatSummary skipped: sub-hour rate limit retries exhausted",
                    hotel_code=hotel_code,
                    error=error_msg,
                    quota_limit=limit,
                    quota_window=window,
                )
                return

            wait_seconds = 65 if window_norm == "minute" else 5
            print(
                f"\n⚠️  Rate limit hit (parsed: {limit} per {window or 'unknown'}). "
                f"Backing off {wait_seconds}s then retrying ({short_retry_count}/{max_short_retries})…"
            )
            logger.warning(
                "StatSummary rate-limited; backing off",
                hotel_code=hotel_code,
                error=error_msg,
                quota_limit=limit,
                quota_window=window,
                wait_seconds=wait_seconds,
                retry_attempt=short_retry_count,
            )
            time.sleep(wait_seconds)


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

    logger.info("Re-processing data", hotel_code=hotel_code, raw_dir=str(raw_data_dir), output_dir=str(hotel_dir))
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
            config_model = HotelConfigResponse(**config_response) if isinstance(config_response, dict) else config_response
            hotel_local_time = config_model.hotel_info.local_time
            if hotel_local_time:
                logger.info("Hotel local time extracted", hotel_code=hotel_code, local_time=str(hotel_local_time))
        except Exception as e:
            logger.warning("Could not extract hotel local time", hotel_code=hotel_code, error=str(e))

        # ==================== LOAD RAW STATDAILY DATA ====================
        print(f"\n2️⃣  Loading raw StatDaily data...")
        raw_statdaily_files = list(raw_data_dir.glob("raw_reservations_*.json"))
        if not raw_statdaily_files:
            print(f"   ❌ No raw StatDaily file found in {raw_data_dir}")
            return 1

        raw_statdaily_file = raw_statdaily_files[0]
        with open(raw_statdaily_file, "r") as f:
            statdaily_records = json.load(f)
        print(f"   ✅ Loaded {len(statdaily_records)} StatDaily records from: {raw_statdaily_file.name}")

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
            processed_file = hotel_dir / f"processed_reservations_reservations-{timestamp_suffix}.json"
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
            print(f"   ✅ Copied StatSummary data: {stat_summary_file.name} ({len(stat_summary_data)} records)")
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
                truncate=True
            )
            print(f"   ✅ Reservations from StatDaily imported to PostgreSQL (table: reservations_from_statdaily)")
        else:
            print(f"   ⚠️  No reservations file found, skipping import")

        # Import StatDaily data
        raw_reservations_files = list(hotel_dir.glob("raw_reservations_*.json"))
        if raw_reservations_files:
            # Use the first file found
            stat_daily_file = raw_reservations_files[0]
            import_stat_daily_to_postgres(
                json_file_path=str(stat_daily_file),
                table_name="stat_daily",
                truncate=True
            )
            print(f"   ✅ StatDaily data imported to PostgreSQL")
        else:
            print(f"   ℹ️  No StatDaily file found, skipping import")

        # Import StatSummary data (validation data)
        stat_summary_files = list(hotel_dir.glob("*stat_summary*.json"))
        if stat_summary_files:
            stat_summary_file = stat_summary_files[0]
            import_stat_summary_to_postgres(
                json_file_path=str(stat_summary_file),
                table_name="stat_summary",
                truncate=True
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
    import_to_db = os.getenv("IMPORT_TO_DB", "true").lower() == "true"
    raw_data_path = os.getenv("RAW_DATA_PATH")
    from_date_override = os.getenv("FROM_DATE") or None
    to_date_override = os.getenv("TO_DATE") or None
    is_first_import_override: bool | None
    raw_is_first = os.getenv("IS_FIRST_IMPORT")
    if raw_is_first is None or raw_is_first == "":
        is_first_import_override = None
    elif raw_is_first.lower() in ("true", "1", "yes"):
        is_first_import_override = True
    elif raw_is_first.lower() in ("false", "0", "no"):
        is_first_import_override = False
    else:
        raise ValueError(
            f"IS_FIRST_IMPORT must be true/false (got {raw_is_first!r})"
        )

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
        import_to_db=import_to_db,
    )

    try:
        orchestrator = LocalTestOrchestrator(
            output_dir=output_dir,
            from_date=from_date_override,
            to_date=to_date_override,
            is_first_import=is_first_import_override,
        )
        _assert_sealed(orchestrator)

        if from_date_override or to_date_override or is_first_import_override is not None:
            logger.info(
                "Local date-range override active",
                from_date=from_date_override,
                to_date=to_date_override,
                is_first_import=is_first_import_override,
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

            # Fetch StatSummary validation data — reuse the per-hotel client
            # the orchestrator built from getIntegration to avoid switching to
            # the .env default key (and burning a separate quota).
            hotel_dir = Path(output_dir) / f"{hotel_code}_{orchestrator.s3_manager.timestamp}"
            fetch_stat_summary(
                hotel_code,
                hotel_dir,
                from_date_override=from_date_override,
                to_date_override=to_date_override,
                client=orchestrator.hotel_api_clients.get(hotel_code),
            )

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
                        fetch_stat_summary(
                            hotel_code,
                            hotel_dir,
                            from_date_override=from_date_override,
                            to_date_override=to_date_override,
                            client=orchestrator.hotel_api_clients.get(hotel_code),
                        )

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
    import_to_db = os.getenv("IMPORT_TO_DB", "true").lower() == "true"
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
        print("Files will be saved locally for inspection (no S3/SQS/ESB)")
        print("=" * 80)
        print("ESB Mode: MOCK (sealed — no real ESB calls possible)")

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
