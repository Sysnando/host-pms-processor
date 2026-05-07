#!/usr/bin/env python3
"""
Quick script to import stat_daily data to PostgreSQL.

Usage:
    python tests/scripts/import_stat_daily.py
    python tests/scripts/import_stat_daily.py --json-file path/to/file.json
    python tests/scripts/import_stat_daily.py --truncate
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()

from src.config.logging import configure_logging
from tests.db.stat_daily_importer import import_stat_daily_to_postgres


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Import stat_daily data to PostgreSQL")
    parser.add_argument(
        "--json-file",
        type=str,
        help="Path to stat_daily JSON file (defaults to latest in data_extracts)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="stat_daily",
        help="Table name (default: stat_daily)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate table before importing",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for inserts (default: 1000)",
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging()

    # Determine JSON file path
    if args.json_file:
        json_file = Path(args.json_file)
    else:
        # Find latest stat_daily_raw.json in data_extracts
        # Pattern matches: HOTELCODE_YYYYMMDD_HHMMSS (e.g., PTLISLSA_20251123_165155)
        data_extracts_dir = Path(__file__).parent.parent.parent / "data_extracts"
        extract_dirs = sorted(
            data_extracts_dir.glob("*_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        if not extract_dirs:
            print("❌ No data extract directories found in data_extracts/")
            print("   Please specify --json-file path")
            return 1

        latest_dir = extract_dirs[0]
        json_file = latest_dir / "08_stat_daily_raw.json"

        if not json_file.exists():
            print(f"❌ stat_daily_raw.json not found in {latest_dir}")
            print("   Please specify --json-file path")
            return 1

        print(f"🎯 Using latest extract: {latest_dir.name}")

    if not json_file.exists():
        print(f"❌ JSON file not found: {json_file}")
        return 1

    print(f"📁 JSON file: {json_file}")
    print(f"📊 Table: {args.table}")
    print(f"🗑️  Truncate: {args.truncate}")
    print(f"📦 Batch size: {args.batch_size}")
    print()

    try:
        count = import_stat_daily_to_postgres(
            json_file_path=str(json_file),
            table_name=args.table,
            truncate=args.truncate,
            batch_size=args.batch_size,
        )

        print()
        print(f"✅ Successfully imported {count} stat_daily records to {args.table}")
        return 0

    except Exception as e:
        print()
        print(f"❌ Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
