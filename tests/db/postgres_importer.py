"""PostgreSQL importer for reservations data."""

import json
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch
from structlog import get_logger

from tests.db.db_utils import get_db_connection

logger = get_logger(__name__)


def create_reservations_table(cursor, table_name: str = "reservations2"):
    """Create reservations table if it doesn't exist.

    Args:
        cursor: psycopg2 cursor object
        table_name: Name of the table to create
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      record_date VARCHAR(255),
      calendar_date DATE,
      calendar_date_start DATE,
      calendar_date_end DATE,
      created_date DATE,
      pax INTEGER,
      reservation_id BIGINT,
      reservation_id_external BIGINT,
      revenue_fb DECIMAL(10, 2),
      revenue_fb_invoice DECIMAL(10, 2),
      revenue_others DECIMAL(10, 2),
      revenue_others_invoice DECIMAL(10, 2),
      revenue_room DECIMAL(10, 2),
      revenue_room_invoice DECIMAL(10, 2),
      rooms INTEGER,
      status INTEGER,
      agency_code VARCHAR(255),
      channel_code VARCHAR(255),
      company_code VARCHAR(255),
      cro_code VARCHAR(255),
      group_code VARCHAR(255),
      package_code VARCHAR(255),
      rate_code VARCHAR(255),
      room_code VARCHAR(255),
      segment_code VARCHAR(255),
      sub_segment_code VARCHAR(255)
    );
    """

    cursor.execute(create_table_sql)
    logger.info("Reservations table created/verified", table_name=table_name)


def import_reservations_to_postgres(
    json_file_path: str,
    table_name: str = "reservations2",
    truncate: bool = False,
    batch_size: int = 1000,
) -> int:
    """Import reservations from JSON file to PostgreSQL.

    Args:
        json_file_path: Path to the JSON file containing reservations
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
        "Starting reservations import",
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

    # Handle both formats: {"reservations": [...]} and direct list
    if isinstance(data, dict) and "reservations" in data:
        reservations = data["reservations"]
    else:
        reservations = data

    if not reservations:
        logger.warning("No reservations found in JSON file")
        return 0

    logger.info("Loaded reservations from JSON", count=len(reservations))

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table if needed
        create_reservations_table(cursor, table_name)

        # Truncate if requested
        if truncate:
            logger.info("Truncating table", table_name=table_name)
            cursor.execute(f"TRUNCATE TABLE {table_name};")

        # Define columns in order
        columns = [
            "record_date",
            "calendar_date",
            "calendar_date_start",
            "calendar_date_end",
            "created_date",
            "pax",
            "reservation_id",
            "reservation_id_external",
            "revenue_fb",
            "revenue_fb_invoice",
            "revenue_others",
            "revenue_others_invoice",
            "revenue_room",
            "revenue_room_invoice",
            "rooms",
            "status",
            "agency_code",
            "channel_code",
            "company_code",
            "cro_code",
            "group_code",
            "package_code",
            "rate_code",
            "room_code",
            "segment_code",
            "sub_segment_code",
        ]

        # Prepare INSERT statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Convert reservations to tuples
        data_tuples = []
        for reservation in reservations:
            values = []
            for col in columns:
                value = reservation.get(col)
                # Convert empty strings to None for proper NULL handling
                if value == "":
                    value = None
                values.append(value)
            data_tuples.append(tuple(values))

        # Insert in batches
        logger.info("Inserting reservations", total=len(data_tuples), batch_size=batch_size)

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
            "Successfully imported reservations",
            imported=len(data_tuples),
            total_in_table=total_count,
            table_name=table_name,
        )

        return len(data_tuples)

    except Exception as e:
        conn.rollback()
        logger.error("Error importing reservations", error=str(e))
        raise

    finally:
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")


if __name__ == "__main__":
    """Example usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Import reservations to PostgreSQL")
    parser.add_argument("json_file", help="Path to JSON file")
    parser.add_argument("--table", default="reservations2", help="Table name")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before import")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")

    args = parser.parse_args()

    count = import_reservations_to_postgres(
        json_file_path=args.json_file,
        table_name=args.table,
        truncate=args.truncate,
        batch_size=args.batch_size,
    )

    print(f"✅ Imported {count} reservations to {args.table}")
