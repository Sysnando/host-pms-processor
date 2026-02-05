"""PostgreSQL importer for stat_daily data."""

import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch
from structlog import get_logger

logger = get_logger(__name__)


def get_db_connection():
    """Get PostgreSQL database connection from environment variables.

    Returns:
        psycopg2 connection object

    Raises:
        ValueError: If required environment variables are missing
        psycopg2.Error: If connection fails
    """
    # Try DATABASE_URL first
    database_url = os.environ.get("DATABASE_URL")

    if database_url:
        logger.info("Connecting to PostgreSQL using DATABASE_URL")
        return psycopg2.connect(database_url)

    # Otherwise, build from individual components
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")

    # Check required fields
    if not db_name:
        raise ValueError("Either DATABASE_URL or DB_NAME environment variable is required")
    if not db_user:
        raise ValueError("Either DATABASE_URL or DB_USER environment variable is required")

    db_config = {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "database": db_name,
        "user": db_user,
        "password": os.environ.get("DB_PASSWORD", ""),
    }

    logger.info(
        "Connecting to PostgreSQL",
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        user=db_config["user"],
    )

    return psycopg2.connect(**db_config)


def create_stat_daily_table(cursor, table_name: str = "stat_daily"):
    """Create stat_daily table if it doesn't exist.

    Args:
        cursor: psycopg2 cursor object
        table_name: Name of the table to create
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      row_number INTEGER,
      total_rows INTEGER,
      record_type VARCHAR(50),
      hotel_date TIMESTAMP,
      res_no INTEGER,
      res_id INTEGER,
      detail_id INTEGER,
      master_detail INTEGER,
      global_res_guest_id INTEGER,
      created_on TIMESTAMP,
      check_in TIMESTAMP,
      check_out TIMESTAMP,
      option_date TIMESTAMP,
      category VARCHAR(100),
      complex_code VARCHAR(50),
      room_name VARCHAR(50),
      agency VARCHAR(100),
      company VARCHAR(100),
      cro VARCHAR(100),
      groupname VARCHAR(100),
      res_status INTEGER,
      guest_id INTEGER,
      country_iso_code VARCHAR(10),
      nationality_iso_code VARCHAR(10),
      pack VARCHAR(100),
      price_list VARCHAR(100),
      segment_description VARCHAR(255),
      sub_segment_description VARCHAR(255),
      channel_description VARCHAR(100),
      additional_status_code VARCHAR(50),
      additional_status_description VARCHAR(255),
      category_upgrade_from VARCHAR(100),
      pax INTEGER,
      children_type1 INTEGER,
      children_type2 INTEGER,
      children_type3 INTEGER,
      room_nights INTEGER,
      charge_code VARCHAR(50),
      sales_group INTEGER,
      sales_group_desc VARCHAR(100),
      revenue_gross DECIMAL(15, 4),
      revenue_net DECIMAL(15, 4)
    );
    """

    cursor.execute(create_table_sql)
    logger.info("StatDaily table created/verified", table_name=table_name)


def import_stat_daily_to_postgres(
    json_file_path: str,
    table_name: str = "stat_daily",
    truncate: bool = False,
    batch_size: int = 1000,
) -> int:
    """Import stat_daily data from JSON file to PostgreSQL.

    Args:
        json_file_path: Path to the JSON file containing stat_daily records
        table_name: Name of the PostgreSQL table
        truncate: Whether to truncate the table before importing
        batch_size: Number of records to insert per batch

    Returns:
        Number of records imported

    Raises:
        FileNotFoundError: If JSON file not found
        json.JSONDecodeError: If JSON file is invalid
        psycopg2.Error: If database operation fails
    """
    logger.info(
        "Starting stat_daily import",
        json_file_path=json_file_path,
        table_name=table_name,
        truncate=truncate,
    )

    # Load JSON data
    json_path = Path(json_file_path)
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file_path}")

    with open(json_path, "r") as f:
        data = json.load(f)

    # Data should be a list of stat_daily records
    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of stat_daily records")

    if not data:
        logger.warning("No stat_daily records found in JSON file")
        return 0

    logger.info("Loaded stat_daily records from JSON", count=len(data))

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table if needed
        create_stat_daily_table(cursor, table_name)

        # Truncate if requested
        if truncate:
            logger.info("Truncating table", table_name=table_name)
            cursor.execute(f"TRUNCATE TABLE {table_name};")

        # Map CamelCase keys to snake_case columns
        camel_to_snake = {
            "RowNumber": "row_number",
            "TotalRows": "total_rows",
            "RecordType": "record_type",
            "HotelDate": "hotel_date",
            "ResNo": "res_no",
            "ResId": "res_id",
            "DetailId": "detail_id",
            "MasterDetail": "master_detail",
            "GlobalResGuestId": "global_res_guest_id",
            "CreatedOn": "created_on",
            "CheckIn": "check_in",
            "CheckOut": "check_out",
            "OptionDate": "option_date",
            "Category": "category",
            "ComplexCode": "complex_code",
            "RoomName": "room_name",
            "Agency": "agency",
            "Company": "company",
            "Cro": "cro",
            "Groupname": "groupname",
            "ResStatus": "res_status",
            "Guest_Id": "guest_id",
            "CountryIsoCode": "country_iso_code",
            "NationalityIsoCode": "nationality_iso_code",
            "Pack": "pack",
            "PriceList": "price_list",
            "SegmentDescription": "segment_description",
            "SubSegmentDescription": "sub_segment_description",
            "ChannelDescription": "channel_description",
            "AdditionalStatusCode": "additional_status_code",
            "AdditionalStatusDescription": "additional_status_description",
            "CategoryUpgradeFrom": "category_upgrade_from",
            "Pax": "pax",
            "ChildrenType1": "children_type1",
            "ChildrenType2": "children_type2",
            "ChildrenType3": "children_type3",
            "RoomNights": "room_nights",
            "ChargeCode": "charge_code",
            "SalesGroup": "sales_group",
            "SalesGroupDesc": "sales_group_desc",
            "RevenueGross": "revenue_gross",
            "RevenueNet": "revenue_net",
        }

        # Define columns in order (snake_case)
        columns = [
            "row_number",
            "total_rows",
            "record_type",
            "hotel_date",
            "res_no",
            "res_id",
            "detail_id",
            "master_detail",
            "global_res_guest_id",
            "created_on",
            "check_in",
            "check_out",
            "option_date",
            "category",
            "complex_code",
            "room_name",
            "agency",
            "company",
            "cro",
            "groupname",
            "res_status",
            "guest_id",
            "country_iso_code",
            "nationality_iso_code",
            "pack",
            "price_list",
            "segment_description",
            "sub_segment_description",
            "channel_description",
            "additional_status_code",
            "additional_status_description",
            "category_upgrade_from",
            "pax",
            "children_type1",
            "children_type2",
            "children_type3",
            "room_nights",
            "charge_code",
            "sales_group",
            "sales_group_desc",
            "revenue_gross",
            "revenue_net",
        ]

        # Prepare INSERT statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Convert stat_daily records to tuples
        data_tuples = []
        for record in data:
            values = []
            for col in columns:
                # Find the corresponding CamelCase key
                camel_key = next((k for k, v in camel_to_snake.items() if v == col), None)
                value = record.get(camel_key) if camel_key else None

                # Convert empty strings to None for proper NULL handling
                if value == "":
                    value = None

                values.append(value)
            data_tuples.append(tuple(values))

        # Insert in batches
        logger.info("Inserting stat_daily records", total=len(data_tuples), batch_size=batch_size)

        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i : i + batch_size]
            execute_batch(cursor, insert_sql, batch)
            logger.debug("Inserted batch", batch_number=i // batch_size + 1, records=len(batch))

        # Commit transaction
        conn.commit()

        # Get final count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_count = cursor.fetchone()[0]

        logger.info(
            "Successfully imported stat_daily records",
            imported=len(data_tuples),
            total_in_table=total_count,
            table_name=table_name,
        )

        return len(data_tuples)

    except Exception as e:
        conn.rollback()
        logger.error("Error importing stat_daily records", error=str(e))
        raise

    finally:
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")


if __name__ == "__main__":
    """Example usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Import stat_daily data to PostgreSQL")
    parser.add_argument("json_file", help="Path to JSON file")
    parser.add_argument("--table", default="stat_daily", help="Table name")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before import")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")

    args = parser.parse_args()

    count = import_stat_daily_to_postgres(
        json_file_path=args.json_file,
        table_name=args.table,
        truncate=args.truncate,
        batch_size=args.batch_size,
    )

    print(f"✅ Imported {count} stat_daily records to {args.table}")
