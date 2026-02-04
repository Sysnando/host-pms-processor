"""
Local script to fetch data from Host PMS API and transform reservations, or re-process existing raw data.

Usage (fetch from API):
    python fetch_and_transform_local.py --hotel-code HOTEL001 --from-date 1970-01-01T00:00:00Z
    python fetch_and_transform_local.py --hotel-code PTLISLSA --from-date 2025-01-01T00:00:00Z

Usage (re-process existing raw data):
    python fetch_and_transform_local.py --raw-data-path PTLISLSA_20251123_165155
    python fetch_and_transform_local.py --raw-data-path data_extracts/PTLISLSA_20251123_165155

Notes:
    - If --raw-data-path is provided: Loads from existing data_extracts directory (skips API calls)
    - If --raw-data-path is not provided: Both --hotel-code and --from-date are required
    - --hotel-code: Hotel code identifier
    - --from-date: Use 1970-01-01T00:00:00Z for full sync, or a recent date for incremental sync
    - --raw-data-path: Directory name or full path in data_extracts (e.g., PTLISLSA_20251123_165155)
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from .env file (for DATABASE_URL and other local testing vars)
from dotenv import load_dotenv
load_dotenv()

from structlog import get_logger
from src.config.logging import configure_logging
from src.clients.host_api_client import HostPMSAPIClient
from src.transformers.config_transformer import ConfigTransformer
from src.transformers.reservation_transformer import ReservationTransformer
from src.transformers.stat_daily_transformer import StatDailyTransformer
from tests.db.sql_generator import generate_sql_from_reservations

# Optional PostgreSQL imports (for local testing only)
try:
    from tests.db.postgres_importer import import_reservations_to_postgres
    from tests.db.stat_daily_importer import import_stat_daily_to_postgres
    DB_IMPORT_AVAILABLE = True
except ImportError:
    DB_IMPORT_AVAILABLE = False
    import_reservations_to_postgres = None
    import_stat_daily_to_postgres = None


def fetch_and_transform_local(hotel_code: str = None, from_date: str = None, raw_data_path: str = None):
    """Fetch data from Host PMS API and save raw and transformed responses locally, or re-process existing raw data."""

    # Configure logging
    configure_logging()
    logger = get_logger(__name__)

    # Create output directory
    output_dir = Path("./data_extracts")
    output_dir.mkdir(exist_ok=True)

    # Determine if we're using raw data or fetching from API
    using_raw_data = raw_data_path is not None

    if using_raw_data:
        # Load from existing raw data directory
        raw_data_dir = Path(raw_data_path)

        # If raw_data_path is a relative path, prepend data_extracts
        if not raw_data_dir.is_absolute():
            raw_data_dir = output_dir / raw_data_path

        if not raw_data_dir.exists():
            print(f"❌ Error: Raw data directory not found: {raw_data_dir}")
            print(f"   Tried path: {raw_data_dir.resolve()}")
            return

        # Extract hotel code from directory name (e.g., PTLISLSA_20251123_165155 -> PTLISLSA)
        dir_name = raw_data_dir.name
        hotel_code = dir_name.split("_")[0]

        # Create new output directory for re-processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hotel_dir = output_dir / f"{raw_data_dir.name}_reprocess_{timestamp}"
        hotel_dir.mkdir(exist_ok=True)

        logger.info("Re-processing data from raw directory", hotel_code=hotel_code, raw_data_dir=str(raw_data_dir), output_dir=str(hotel_dir))
    else:
        # Fetch from API
        if not hotel_code or not from_date:
            print("❌ Error: --hotel-code and --from-date are required when not using --raw-data-path")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hotel_dir = output_dir / f"{hotel_code}_{timestamp}"
        hotel_dir.mkdir(exist_ok=True)

        client = HostPMSAPIClient()

        logger.info("Starting data extraction", hotel_code=hotel_code, output_dir=str(hotel_dir), from_date=from_date)

    # ==================== CONFIG ====================
    logger.info("Loading hotel config", hotel_code=hotel_code)
    try:
        if using_raw_data:
            # Load from existing file
            config_raw_file = raw_data_dir / "01_config_raw.json"
            if not config_raw_file.exists():
                logger.warning("Config file not found", hotel_code=hotel_code, file_path=str(config_raw_file))
                config_response = None
            else:
                with open(config_raw_file, "r") as f:
                    config_response = json.load(f)
                logger.info("Raw config loaded", hotel_code=hotel_code, file_path=str(config_raw_file))
        else:
            # Fetch from API
            config_response = client.get_hotel_config(hotel_code)

            # Save raw config response
            config_raw_file = hotel_dir / "01_config_raw.json"
            with open(config_raw_file, "w") as f:
                json.dump(config_response, f, indent=2)
            logger.info("Raw config saved", hotel_code=hotel_code, file_path=str(config_raw_file))

        if config_response is not None:
            # Transform config
            hotel_config, segment_collection = ConfigTransformer.transform(config_response)

            # Save transformed config
            config_transformed_file = hotel_dir / "01_config_transformed.json"
            config_data = {
                "hotel_config": json.loads(hotel_config.model_dump_json()),
                "segments": json.loads(segment_collection.model_dump_json())
            }
            with open(config_transformed_file, "w") as f:
                json.dump(config_data, f, indent=2)
            logger.info("Transformed config saved", hotel_code=hotel_code, file_path=str(config_transformed_file))

            # Extract and save room inventory
            room_inventory = ConfigTransformer.get_room_inventory(config_response)
            inventory_transformed_file = hotel_dir / "02_inventory_transformed.json"
            with open(inventory_transformed_file, "w") as f:
                json.dump(json.loads(room_inventory.model_dump_json()), f, indent=2)
            logger.info("Room inventory saved", hotel_code=hotel_code, file_path=str(inventory_transformed_file))
        else:
            logger.warning("Skipping config transformation (config not loaded)", hotel_code=hotel_code)

    except Exception as e:
        logger.error("Error with config", hotel_code=hotel_code, error=str(e))
        return

    # Extract reservation status mappings and hotel local time from config
    reservation_statuses = {}
    hotel_local_time = None
    try:
        from src.models.host.config import HotelConfigResponse

        # Convert dict to HotelConfigResponse if needed
        if isinstance(config_response, dict):
            config_model = HotelConfigResponse(**config_response)
        else:
            config_model = config_response

        reservation_statuses = ConfigTransformer.get_reservation_statuses(config_model)
        logger.info("Found reservation status codes", hotel_code=hotel_code, status_count=len(reservation_statuses))

        # Extract hotel local time for record_date calculation
        hotel_local_time = config_model.hotel_info.local_time
        if hotel_local_time:
            logger.info("Hotel local time extracted", hotel_code=hotel_code, local_time=str(hotel_local_time))
    except Exception as e:
        logger.warning("Could not extract reservation statuses", hotel_code=hotel_code, error=str(e))

    # ==================== RESERVATIONS ====================
    # logger.info("Loading reservations", hotel_code=hotel_code)
    # try:
    #     if using_raw_data:
    #         # Load from existing file
    #         reservations_raw_file = raw_data_dir / "03_reservations_raw.json"
    #         if not reservations_raw_file.exists():
    #             logger.warning("Reservations file not found", hotel_code=hotel_code, file_path=str(reservations_raw_file))
    #             reservations_response = None
    #         else:
    #             with open(reservations_raw_file, "r") as f:
    #                 reservations_response = json.load(f)
    #             reservations_list = reservations_response.get("Reservations", [])
    #             logger.info("Raw reservations loaded", hotel_code=hotel_code, file_path=str(reservations_raw_file), total_records=len(reservations_list))
    #     else:
    #         # Fetch from API
    #         reservations_response = client.get_reservations(hotel_code, update_from=from_date)

    #         # Save raw reservations response
    #         reservations_raw_file = hotel_dir / "03_reservations_raw.json"
    #         with open(reservations_raw_file, "w") as f:
    #             json.dump(reservations_response, f, indent=2)

    #         # Log pagination details
    #         reservations_list = reservations_response.get("Reservations", [])
    #         total_rows = reservations_list[0].get("TotalRows") if reservations_list else 0
    #         logger.info("Raw reservations saved", hotel_code=hotel_code, file_path=str(reservations_raw_file), total_records=len(reservations_list), total_rows=total_rows if total_rows else None)

    #     # Transform reservations (API returns "Reservations" with capital R)
    #     if reservations_response is None:
    #         logger.warning("Skipping reservation transformation (reservations not loaded)", hotel_code=hotel_code)
    #         reservation_list = []
    #     else:
    #         reservation_list = reservations_response.get("Reservations", [])

    #     if reservation_list:
    #         reservation_collection, skipped_duplicates, composite_ids, overlap_records = ReservationTransformer.transform_batch(
    #             reservation_list, hotel_code, reservation_statuses, hotel_local_time
    #         )

    #         # Save transformed reservations
    #         reservations_transformed_file = hotel_dir / "03_reservations_transformed.json"
    #         with open(reservations_transformed_file, "w") as f:
    #             json.dump(json.loads(reservation_collection.model_dump_json()), f, indent=2)
    #         logger.info("Transformed reservations saved", hotel_code=hotel_code, file_path=str(reservations_transformed_file))

    #         # Save overlap records to file
    #         if overlap_records:
    #             overlap_file = hotel_dir / "04_overlap_records.json"
    #             with open(overlap_file, "w") as f:
    #                 json.dump(overlap_records, f, indent=2)
    #             print(f"   ✅ Overlap records saved: {overlap_file} ({len(overlap_records)} records)")
    #         else:
    #             print(f"   ℹ️  No overlap records")

    #         # Find all Price entries where date >= checkout date
    #         prices_beyond_checkout = []
    #         for reservation_dict in reservation_list:
    #             checkout_date = reservation_dict.get("CheckOut")
    #             prices = reservation_dict.get("Prices", [])

    #             # Parse checkout date if it's a string
    #             if isinstance(checkout_date, str):
    #                 try:
    #                     checkout_dt = datetime.fromisoformat(checkout_date.replace('Z', '+00:00'))
    #                 except Exception:
    #                     continue
    #             elif isinstance(checkout_date, datetime):
    #                 checkout_dt = checkout_date
    #             else:
    #                 continue

    #             for price in prices:
    #                 price_date = price.get("Date")

    #                 # Parse price date if it's a string
    #                 if isinstance(price_date, str):
    #                     try:
    #                         price_dt = datetime.fromisoformat(price_date.replace('Z', '+00:00'))
    #                     except Exception:
    #                         continue
    #                 elif isinstance(price_date, datetime):
    #                     price_dt = price_date
    #                 else:
    #                     continue

    #                 # Check if price date >= checkout date (comparing date parts only)
    #                 if price_dt.date() >= checkout_dt.date():
    #                     prices_beyond_checkout.append({
    #                         "reservation_id": reservation_dict.get("ResId"),
    #                         "reservation_no": reservation_dict.get("ResNo"),
    #                         "global_res_guest_id": reservation_dict.get("GlobalResGuestId"),
    #                         "checkout_date": checkout_date if isinstance(checkout_date, str) else checkout_date.isoformat(),
    #                         "price": price
    #                     })

    #         # Save prices beyond checkout to file
    #         if prices_beyond_checkout:
    #             prices_beyond_checkout_file = hotel_dir / "05_prices_beyond_checkout.json"
    #             with open(prices_beyond_checkout_file, "w") as f:
    #                 json.dump(prices_beyond_checkout, f, indent=2)
    #             print(f"   ✅ Prices beyond checkout saved: {prices_beyond_checkout_file} ({len(prices_beyond_checkout)} price entries)")
    #         else:
    #             print(f"   ℹ️  No price entries found with dates >= checkout")

    #         # Find reservations with missing price entries during the stay period
    #         reservations_missing_prices = []
    #         same_day_reservations = []

    #         for reservation_dict in reservation_list:
    #             checkin_date = reservation_dict.get("CheckIn")
    #             checkout_date = reservation_dict.get("CheckOut")
    #             prices = reservation_dict.get("Prices", [])

    #             # Parse dates
    #             try:
    #                 if isinstance(checkin_date, str):
    #                     checkin_dt = datetime.fromisoformat(checkin_date.replace('Z', '+00:00'))
    #                 elif isinstance(checkin_date, datetime):
    #                     checkin_dt = checkin_date
    #                 else:
    #                     continue

    #                 if isinstance(checkout_date, str):
    #                     checkout_dt = datetime.fromisoformat(checkout_date.replace('Z', '+00:00'))
    #                 elif isinstance(checkout_date, datetime):
    #                     checkout_dt = checkout_date
    #                 else:
    #                     continue
    #             except Exception:
    #                 continue

    #             # Check for same-day check-in/check-out
    #             if checkin_dt.date() == checkout_dt.date():
    #                 same_day_reservations.append({
    #                     "reservation_id": reservation_dict.get("ResId"),
    #                     "reservation_no": reservation_dict.get("ResNo"),
    #                     "global_res_guest_id": reservation_dict.get("GlobalResGuestId"),
    #                     "checkin": checkin_date if isinstance(checkin_date, str) else checkin_date.isoformat(),
    #                     "checkout": checkout_date if isinstance(checkout_date, str) else checkout_date.isoformat(),
    #                     "rooms": reservation_dict.get("Rooms"),
    #                     "price_count": len(prices),
    #                     "reservation": reservation_dict
    #                 })
    #                 continue  # Skip further processing for same-day reservations

    #             # Calculate expected stay dates (CheckIn to CheckOut-1)
    #             expected_dates = set()
    #             current_date = checkin_dt.date()
    #             checkout_date_only = checkout_dt.date()

    #             while current_date < checkout_date_only:
    #                 expected_dates.add(current_date.isoformat())
    #                 current_date += timedelta(days=1)

    #             # Get actual price dates
    #             actual_price_dates = set()
    #             for price in prices:
    #                 price_date = price.get("Date")
    #                 try:
    #                     if isinstance(price_date, str):
    #                         price_dt = datetime.fromisoformat(price_date.replace('Z', '+00:00'))
    #                     elif isinstance(price_date, datetime):
    #                         price_dt = price_date
    #                     else:
    #                         continue
    #                     actual_price_dates.add(price_dt.date().isoformat())
    #                 except Exception:
    #                     continue

    #             # Find missing dates
    #             missing_dates = expected_dates - actual_price_dates

    #             if missing_dates:
    #                 reservations_missing_prices.append({
    #                     "reservation_id": reservation_dict.get("ResId"),
    #                     "reservation_no": reservation_dict.get("ResNo"),
    #                     "global_res_guest_id": reservation_dict.get("GlobalResGuestId"),
    #                     "checkin": checkin_date if isinstance(checkin_date, str) else checkin_date.isoformat(),
    #                     "checkout": checkout_date if isinstance(checkout_date, str) else checkout_date.isoformat(),
    #                     "expected_dates": sorted(list(expected_dates)),
    #                     "actual_price_dates": sorted(list(actual_price_dates)),
    #                     "missing_dates": sorted(list(missing_dates)),
    #                     "missing_count": len(missing_dates),
    #                     "reservation": reservation_dict
    #                 })

    #         # Save reservations with missing prices to file
    #         if reservations_missing_prices:
    #             missing_prices_file = hotel_dir / "06_reservations_missing_prices.json"
    #             with open(missing_prices_file, "w") as f:
    #                 json.dump(reservations_missing_prices, f, indent=2)
    #             print(f"   ✅ Reservations with missing prices saved: {missing_prices_file} ({len(reservations_missing_prices)} reservations)")
    #         else:
    #             print(f"   ℹ️  No reservations found with missing price entries")

    #         # Save same-day check-in/check-out reservations to file
    #         if same_day_reservations:
    #             same_day_file = hotel_dir / "07_reservations_same_day.json"
    #             with open(same_day_file, "w") as f:
    #                 json.dump(same_day_reservations, f, indent=2)
    #             print(f"   ✅ Same-day reservations saved: {same_day_file} ({len(same_day_reservations)} reservations)")
    #         else:
    #             print(f"   ℹ️  No same-day check-in/check-out reservations found")

    #         # Generate SQL INSERT script from the transformed reservations
    #         reservations_list = reservation_collection.reservations
    #         if reservations_list:
    #             sql_file = generate_sql_from_reservations(
    #                 [r.model_dump() for r in reservations_list],
    #                 hotel_dir
    #             )
    #             if sql_file:
    #                 logger.info("SQL script saved", hotel_code=hotel_code, file_name=sql_file.name)
    #     else:
    #         logger.info("No reservations found", hotel_code=hotel_code)

    # except Exception as e:
    #     logger.error("Error fetching reservations", hotel_code=hotel_code, error=str(e))

    # ==================== STAT DAILY (INVOICE DATA) ====================
    logger.info("Loading StatDaily data", hotel_code=hotel_code)
    try:
        # Calculate date range: 30 days ago to yesterday
        today = datetime.now().date()
        yesterday = today - timedelta(days=30)
        start_date = today - timedelta(days=95)

        print(f"   📅 Date range: {start_date} to {yesterday}")

        # Fetch StatDaily for each date in range
        all_stat_daily_records = []
        dates_to_fetch = []
        current_date = start_date

        while current_date <= yesterday:
            dates_to_fetch.append(current_date)
            current_date += timedelta(days=1)

        print(f"   📊 Fetching {len(dates_to_fetch)} days of StatDaily data...")

        # Load existing StatDaily data if using raw data
        if using_raw_data:
            stat_daily_raw_file = raw_data_dir / "08_stat_daily_raw.json"
            if stat_daily_raw_file.exists():
                with open(stat_daily_raw_file, "r") as f:
                    all_stat_daily_records = json.load(f)
                print(f"   ✅ Loaded existing StatDaily data: {len(all_stat_daily_records)} records")
            else:
                print(f"   ⚠️  StatDaily file not found in raw data directory")
        else:
            # Fetch from API
            for date in dates_to_fetch:
                date_str = date.isoformat()
                try:
                    stat_daily_response = client.get_stat_daily(hotel_date_filter=date_str)

                    # Response is a list
                    if isinstance(stat_daily_response, list):
                        all_stat_daily_records.extend(stat_daily_response)
                        print(f"   ✅ {date_str}: {len(stat_daily_response)} records")
                    else:
                        print(f"   ⚠️  {date_str}: No data")

                except Exception as e:
                    print(f"   ⚠️  {date_str}: Failed to fetch ({str(e)})")
                    continue

        # Save raw StatDaily data
        if all_stat_daily_records:
            stat_daily_raw_file = hotel_dir / "08_stat_daily_raw.json"
            with open(stat_daily_raw_file, "w") as f:
                json.dump(all_stat_daily_records, f, indent=2)
            print(f"   ✅ Raw StatDaily saved: {stat_daily_raw_file} ({len(all_stat_daily_records)} records)")

            # ==================== CONVERT STAT DAILY TO RESERVATIONS ====================
            print(f"\n   🔄 Converting StatDaily to Climber reservations...")
            try:
                from src.transformers.stat_daily_to_reservation_transformer import StatDailyToReservationTransformer

                reservation_collection = StatDailyToReservationTransformer.transform_batch(
                    all_stat_daily_records,
                    hotel_code=hotel_code,
                    hotel_local_time=hotel_local_time
                )

                # Save reservations from StatDaily
                reservations_from_statdaily_file = hotel_dir / "12_reservations_from_statdaily.json"
                with open(reservations_from_statdaily_file, "w") as f:
                    json.dump(json.loads(reservation_collection.model_dump_json()), f, indent=2)

                print(f"   ✅ Reservations from StatDaily saved: {reservations_from_statdaily_file}")
                print(f"   📊 Created {len(reservation_collection.reservations)} reservation lines from StatDaily")

                # Generate SQL INSERT script from StatDaily reservations
                if reservation_collection.reservations:
                    sql_file = generate_sql_from_reservations(
                        [r.model_dump() for r in reservation_collection.reservations],
                        hotel_dir,
                    )
                    if sql_file:
                        print(f"   ✅ SQL script saved: {sql_file.name}")

            except Exception as e:
                print(f"   ❌ Error converting StatDaily to reservations: {str(e)}")
                import traceback
                traceback.print_exc()

            # Process StatDaily and update reservations
            if reservation_list and 'reservation_collection' in locals():
                print(f"\n   🔄 Applying StatDaily invoice data to reservations...")
                updated_collection, stats = StatDailyTransformer.process_stat_daily_for_reservations(
                    all_stat_daily_records,
                    reservation_collection
                )

                # Save updated reservations
                reservations_with_invoices_file = hotel_dir / "09_reservations_with_invoices.json"
                with open(reservations_with_invoices_file, "w") as f:
                    json.dump(json.loads(updated_collection.model_dump_json()), f, indent=2)
                print(f"   ✅ Updated reservations saved: {reservations_with_invoices_file}")
                print(f"   📊 Updated {stats['updated_reservations']}/{stats['total_reservations']} reservations ({stats['match_rate']})")

                # Save consolidated records separately for easier inspection
                if stats.get('consolidated_records'):
                    consolidated_file = hotel_dir / "10_stat_daily_consolidated.json"
                    # Convert datetime objects to ISO format strings for JSON serialization
                    consolidated_records_serializable = []
                    for record in stats['consolidated_records']:
                        serializable_record = record.copy()
                        if isinstance(serializable_record.get('hotel_date'), datetime):
                            serializable_record['hotel_date'] = serializable_record['hotel_date'].isoformat()
                        consolidated_records_serializable.append(serializable_record)

                    with open(consolidated_file, "w") as f:
                        json.dump(consolidated_records_serializable, f, indent=2)
                    print(f"   ✅ Consolidated StatDaily records saved: {consolidated_file} ({len(consolidated_records_serializable)} records)")

                # Save processing stats (remove consolidated_records from stats to keep it clean)
                stats_for_file = stats.copy()
                stats_for_file.pop('consolidated_records', None)  # Remove to avoid duplication

                stats_file = hotel_dir / "11_stat_daily_stats.json"
                with open(stats_file, "w") as f:
                    json.dump(stats_for_file, f, indent=2)
                print(f"   ✅ StatDaily stats saved: {stats_file}")

                # Update the reservation_collection for downstream use
                reservation_collection = updated_collection

                # Re-generate SQL with updated data
                reservations_list = reservation_collection.reservations
                if reservations_list:
                    sql_file = generate_sql_from_reservations(
                        [r.model_dump() for r in reservations_list],
                        hotel_dir
                    )
                    if sql_file:
                        print(f"   ✅ Updated SQL script saved: {sql_file.name}")
            else:
                print(f"   ⚠️  No reservations to update with StatDaily data")

        else:
            print(f"   ℹ️  No StatDaily data fetched")

    except Exception as e:
        print(f"   ❌ Error with StatDaily: {str(e)}")

    # ==================== INVENTORY ====================
    logger.info("Loading inventory grid", hotel_code=hotel_code)
    try:
        if using_raw_data:
            # Load from existing file
            inventory_raw_file = raw_data_dir / "02_inventory_raw.json"
            if not inventory_raw_file.exists():
                logger.warning("Inventory file not found", hotel_code=hotel_code, file_path=str(inventory_raw_file))
            else:
                with open(inventory_raw_file, "r") as f:
                    inventory_response = json.load(f)
                logger.info("Raw inventory loaded", hotel_code=hotel_code, file_path=str(inventory_raw_file))
        else:
            # Fetch from API
            inventory_response = client.get_inventory(hotel_code)

            # Save raw inventory response
            inventory_raw_file = hotel_dir / "02_inventory_raw.json"
            with open(inventory_raw_file, "w") as f:
                json.dump(inventory_response, f, indent=2)
            logger.info("Raw inventory saved", hotel_code=hotel_code, file_path=str(inventory_raw_file))

    except Exception as e:
        logger.error("Error with inventory", hotel_code=hotel_code, error=str(e))

    # ==================== IMPORT TO POSTGRESQL ====================
    if DB_IMPORT_AVAILABLE:
        # Check if DATABASE_URL is configured
        import os
        database_url = os.environ.get("DATABASE_URL")
        db_name = os.environ.get("DB_NAME")

        if not database_url and not db_name:
            print("\n5️⃣  PostgreSQL import skipped (DATABASE_URL not configured)")
            print("   ℹ️  To enable database import, set DATABASE_URL environment variable:")
            print('   export DATABASE_URL="postgresql://user:pass@localhost:5432/database"')
        else:
            print("\n5️⃣  Importing data to PostgreSQL...")
            try:
                # Import reservations - priority order:
                # 1. StatDaily-generated reservations (12_reservations_from_statdaily.json)
                # 2. Reservations with invoices (09_reservations_with_invoices.json)
                # 3. Transformed reservations (03_reservations_transformed.json)
                reservations_from_statdaily_file = hotel_dir / "12_reservations_from_statdaily.json"
                reservations_with_invoices_file = hotel_dir / "09_reservations_with_invoices.json"
                reservations_transformed_file = hotel_dir / "03_reservations_transformed.json"

                if reservations_from_statdaily_file.exists():
                    import_reservations_to_postgres(
                        json_file_path=str(reservations_from_statdaily_file),
                        table_name="reservations_from_statdaily",
                        truncate=True
                    )
                    print(f"   ✅ Reservations from StatDaily imported to PostgreSQL (table: reservations_from_statdaily)")
                elif reservations_with_invoices_file.exists():
                    import_reservations_to_postgres(
                        json_file_path=str(reservations_with_invoices_file),
                        table_name="reservations2",
                        truncate=True
                    )
                    print(f"   ✅ Reservations (with invoices) imported to PostgreSQL")
                elif reservations_transformed_file.exists():
                    import_reservations_to_postgres(
                        json_file_path=str(reservations_transformed_file),
                        table_name="reservations2",
                        truncate=True
                    )
                    print(f"   ✅ Reservations imported to PostgreSQL")
                else:
                    print(f"   ⚠️  No reservations file found, skipping import")

                # Import StatDaily data
                stat_daily_raw_file = hotel_dir / "08_stat_daily_raw.json"
                if stat_daily_raw_file.exists():
                    import_stat_daily_to_postgres(
                        json_file_path=str(stat_daily_raw_file),
                        table_name="stat_daily",
                        truncate=True
                    )
                    print(f"   ✅ StatDaily data imported to PostgreSQL")
                else:
                    print(f"   ℹ️  No StatDaily file found, skipping import")

            except Exception as e:
                print(f"   ❌ Error importing to PostgreSQL: {str(e)}")
                print(f"   ℹ️  Database import failed, but data was successfully saved to files")
    else:
        print("\n5️⃣  PostgreSQL import skipped (db module not available)")

    # ==================== SUMMARY ====================
    print(f"\n✨ Data extraction complete!")
    print(f"📁 All files saved to: {hotel_dir}")
    print("\n📋 Files created:")
    for file in sorted(hotel_dir.glob("*.json")):
        size_kb = file.stat().st_size / 1024
        print(f"   - {file.name} ({size_kb:.1f} KB)")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch data from Host PMS API and save raw + transformed responses locally, or re-process existing raw data"
    )
    parser.add_argument(
        "--hotel-code",
        type=str,
        required=False,
        help="Hotel code to extract data for (e.g., HOTEL001) - required if --raw-data-path not provided"
    )
    parser.add_argument(
        "--from-date",
        type=str,
        required=False,
        help="From date for incremental sync (e.g., 1970-01-01T00:00:00Z for full sync) - required if --raw-data-path not provided"
    )
    parser.add_argument(
        "--raw-data-path",
        type=str,
        required=False,
        help="Path to raw data directory in data_extracts (e.g., PTLISLSA_20251123_165155 or data_extracts/PTLISLSA_20251123_165155) - if provided, skips API calls"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.raw_data_path is None:
        if not args.hotel_code or not args.from_date:
            parser.error("Either --raw-data-path OR both --hotel-code and --from-date must be provided")

    fetch_and_transform_local(
        hotel_code=args.hotel_code,
        from_date=args.from_date,
        raw_data_path=args.raw_data_path
    )


if __name__ == "__main__":
    main()
