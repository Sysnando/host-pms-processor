"""Bootstrap the database/schema/tables for a single hotel before running
``tests/test_local_run.py``.

This helper is intended to be invoked by the ``prepare-local-test`` skill,
but it is also safe to run by hand:

    uv run python -m tests.db.setup_hotel_schema --hotel-schema quatro_vias
    uv run python -m tests.db.setup_hotel_schema --hotel-schema quatro_vias --truncate
    uv run python -m tests.db.setup_hotel_schema --hotel-schema quatro_vias --report

Behavior:
    * Reads ``DATABASE_URL`` (or ``DB_*`` env vars) from ``.env``.
    * Connects to the ``postgres`` maintenance database and creates the target
      database if it does not exist.
    * Connects to the target database, creates the per-hotel schema if needed,
      and creates the three test tables inside it by reusing the existing
      ``create_*_table`` helpers from the importers.
    * With ``--truncate``: truncates the three tables in the hotel's schema.
    * Always prints a final single-line JSON document with row counts so the
      skill can parse it without scraping logs.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Optional
from urllib.parse import urlparse, urlunparse

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from structlog import get_logger

from tests.db.postgres_importer import create_reservations_table
from tests.db.stat_daily_importer import create_stat_daily_table
from tests.db.stat_summary_importer import create_stat_summary_table

logger = get_logger(__name__)

TABLES = ("reservations2", "stat_daily", "stat_summary")


def _build_conn_kwargs() -> tuple[dict, str]:
    """Return (connect_kwargs, target_db_name) derived from env vars.

    Prefers ``DATABASE_URL``; falls back to individual ``DB_*`` vars.
    """
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        parsed = urlparse(database_url)
        if not parsed.path or parsed.path == "/":
            raise ValueError("DATABASE_URL must include a database name in the path")
        target_db = parsed.path.lstrip("/")
        return {"dsn": database_url}, target_db

    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    if not db_name or not db_user:
        raise ValueError(
            "Either DATABASE_URL or DB_NAME/DB_USER must be set in the environment"
        )
    return (
        {
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": int(os.environ.get("DB_PORT", "5432")),
            "user": db_user,
            "password": os.environ.get("DB_PASSWORD", ""),
            "dbname": db_name,
        },
        db_name,
    )


def _maintenance_connect(target_db: str):
    """Connect to the ``postgres`` maintenance database using the same creds."""
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        parsed = urlparse(database_url)
        maint = parsed._replace(path="/postgres")
        return psycopg2.connect(urlunparse(maint))

    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD", ""),
        dbname="postgres",
    )


def ensure_database(target_db: str) -> bool:
    """Create the target database if it doesn't exist. Returns True if created."""
    conn = _maintenance_connect(target_db)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            if cur.fetchone():
                return False
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db))
            )
            logger.info("Created database", database=target_db)
            return True
    finally:
        conn.close()


def _connect_target(conn_kwargs: dict):
    """Connect to the target database (not via get_db_connection, because we
    must NOT have HOTEL_SCHEMA short-circuit search_path before the schema
    exists)."""
    return psycopg2.connect(**conn_kwargs)


def ensure_schema_and_tables(conn, schema: str) -> None:
    """Create the schema if missing, then create the three tables inside it."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
        cur.execute(
            sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema))
        )
        create_reservations_table(cur)
        create_stat_daily_table(cur)
        create_stat_summary_table(cur)
    conn.commit()
    logger.info("Schema and tables ready", schema=schema)


def truncate_tables(conn, schema: str) -> None:
    """Truncate the three tables in the hotel's schema."""
    qualified = sql.SQL(", ").join(
        sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(t))
        for t in TABLES
    )
    with conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY").format(qualified))
    conn.commit()
    logger.info("Tables truncated", schema=schema, tables=list(TABLES))


def count_rows(conn, schema: str) -> dict[str, int]:
    """Return row counts for each test table in the hotel's schema."""
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            counts[table] = cur.fetchone()[0]
    return counts


def sanitize_schema(raw: str) -> str:
    """Coerce an arbitrary hotel identifier into a valid lowercase schema name."""
    if not raw:
        raise ValueError("schema name cannot be empty")
    cleaned = re.sub(r"[^a-z0-9_]", "_", raw.lower())
    if cleaned[0].isdigit():
        cleaned = f"h_{cleaned}"
    return cleaned


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hotel-schema",
        required=True,
        help="Schema name for the hotel (raw value; will be sanitized).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the three tables in the hotel's schema before reporting counts.",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Skip create/truncate; only report current row counts.",
    )
    args = parser.parse_args(argv)

    load_dotenv()

    schema = sanitize_schema(args.hotel_schema)

    conn_kwargs, target_db = _build_conn_kwargs()

    if not args.report:
        ensure_database(target_db)

    conn = _connect_target(conn_kwargs)
    try:
        if not args.report:
            ensure_schema_and_tables(conn, schema)
            if args.truncate:
                truncate_tables(conn, schema)
        counts = count_rows(conn, schema)
    finally:
        conn.close()

    # Single-line JSON on stdout for the skill to parse.
    print(json.dumps({"schema": schema, "database": target_db, "counts": counts}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
