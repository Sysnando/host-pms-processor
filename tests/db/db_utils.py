"""Shared database utilities for PostgreSQL importers."""

import os

import psycopg2
from psycopg2 import sql
from structlog import get_logger

logger = get_logger(__name__)


def _apply_hotel_schema(conn) -> None:
    """If HOTEL_SCHEMA is set, restrict the session's search_path to that schema.

    All importers use unqualified table names (e.g. `reservations2`), so setting
    search_path here is enough to direct CREATE/INSERT into the hotel's schema.
    """
    schema = os.environ.get("HOTEL_SCHEMA")
    if not schema:
        return

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema))
        )
    conn.commit()
    logger.info("Applied per-hotel search_path", schema=schema)


def get_db_connection():
    """Get PostgreSQL database connection from environment variables.

    This function tries to connect using DATABASE_URL first, then falls back
    to individual environment variables (DB_NAME, DB_USER, DB_HOST, etc.).

    If the ``HOTEL_SCHEMA`` environment variable is set, the connection's
    ``search_path`` is restricted to that schema (plus ``public``) so all
    table operations resolve inside the hotel's schema.

    Environment Variables:
        DATABASE_URL: Full PostgreSQL connection string (preferred)
        DB_NAME: Database name (required if DATABASE_URL not set)
        DB_USER: Database user (required if DATABASE_URL not set)
        DB_HOST: Database host (default: localhost)
        DB_PORT: Database port (default: 5432)
        DB_PASSWORD: Database password (default: empty string)
        HOTEL_SCHEMA: Optional per-hotel schema name (set search_path on connect)

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
        conn = psycopg2.connect(database_url)
        _apply_hotel_schema(conn)
        return conn

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

    conn = psycopg2.connect(**db_config)
    _apply_hotel_schema(conn)
    return conn
