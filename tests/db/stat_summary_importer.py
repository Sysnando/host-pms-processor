"""PostgreSQL importer for stat_summary data.

This importer is used for testing/validation purposes only.
It imports StatSummary data (aggregated daily statistics) to compare
against the transformed StatDaily data.
"""

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


def create_stat_summary_table(cursor, table_name: str = "stat_summary"):
    """Create stat_summary table if it doesn't exist.

    Args:
        cursor: psycopg2 cursor object
        table_name: Name of the table to create
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      hotel_date TIMESTAMP NOT NULL,
      room_nights INTEGER,
      revenue_net_room DECIMAL(15, 4),
      revenue_net_other DECIMAL(15, 4),
      checksum INTEGER,
      PRIMARY KEY (hotel_date)
    );
    """

    cursor.execute(create_table_sql)
    logger.info("StatSummary table created/verified", table_name=table_name)


def import_stat_summary_to_postgres(
    json_file_path: str,
    table_name: str = "stat_summary",
    truncate: bool = True,
    batch_size: int = 1000,
) -> int:
    """Import stat_summary data from JSON file to PostgreSQL.

    Note: Always truncates the table before import by default since this is
    validation data that should match the date range of the StatDaily fetch.

    Args:
        json_file_path: Path to the JSON file containing stat_summary records
        table_name: Name of the PostgreSQL table
        truncate: Whether to truncate the table before importing (default: True)
        batch_size: Number of records to insert per batch

    Returns:
        Number of records imported

    Raises:
        FileNotFoundError: If JSON file not found
        json.JSONDecodeError: If JSON file is invalid
        psycopg2.Error: If database operation fails
    """
    logger.info(
        "Starting stat_summary import",
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

    # Data should be a list of stat_summary records
    if not isinstance(data, list):
        raise ValueError("JSON file must contain a list of stat_summary records")

    if not data:
        logger.warning("No stat_summary records found in JSON file")
        return 0

    logger.info("Loaded stat_summary records from JSON", count=len(data))

    # Connect to database
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table if needed
        create_stat_summary_table(cursor, table_name)

        # Truncate if requested
        if truncate:
            logger.info("Truncating table", table_name=table_name)
            cursor.execute(f"TRUNCATE TABLE {table_name};")

        # Map CamelCase keys to snake_case columns
        camel_to_snake = {
            "hoteldate": "hotel_date",
            "RoomNights": "room_nights",
            "RevenueNet_Room": "revenue_net_room",
            "RevenueNet_Other": "revenue_net_other",
            "Checksum": "checksum",
        }

        # Define columns in order (snake_case)
        columns = [
            "hotel_date",
            "room_nights",
            "revenue_net_room",
            "revenue_net_other",
            "checksum",
        ]

        # Prepare INSERT statement
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Convert stat_summary records to tuples
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
        logger.info("Inserting stat_summary records", total=len(data_tuples), batch_size=batch_size)

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
            "Successfully imported stat_summary records",
            imported=len(data_tuples),
            total_in_table=total_count,
            table_name=table_name,
        )

        return len(data_tuples)

    except Exception as e:
        conn.rollback()
        logger.error("Error importing stat_summary records", error=str(e))
        raise

    finally:
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")


if __name__ == "__main__":
    """Example usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Import stat_summary data to PostgreSQL")
    parser.add_argument("json_file", help="Path to JSON file")
    parser.add_argument("--table", default="stat_summary", help="Table name")
    parser.add_argument("--truncate", action="store_true", default=True, help="Truncate table before import (default: True)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for inserts")

    args = parser.parse_args()

    count = import_stat_summary_to_postgres(
        json_file_path=args.json_file,
        table_name=args.table,
        truncate=args.truncate,
        batch_size=args.batch_size,
    )

    print(f"✅ Imported {count} stat_summary records to {args.table}")
