#!/usr/bin/env python3
"""
Quick script to import reservations data to PostgreSQL.

Usage (from the repository root):
    python tests/scripts/import_reservations.py
    python tests/scripts/import_reservations.py --json-file path/to/file.json
    python tests/scripts/import_reservations.py --truncate
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

from tests.db.postgres_importer import import_reservations_to_postgres
from src.config.logging import configure_logging


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Import reservations to PostgreSQL")
    parser.add_argument(
        "--json-file",
        type=str,
        help="Path to reservations JSON file (defaults to latest in data_extracts)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="reservations2",
        help="Table name (default: reservations2)",
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
        # Find latest reservations file in data_extracts
        # Prefer file with invoices if available
        data_extracts_dir = Path(__file__).parent.parent.parent / "data_extracts"
        extract_dirs = sorted(
            data_extracts_dir.glob("PTLISLSA_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        if not extract_dirs:
            print("❌ No data extract directories found in data_extracts/")
            print("   Please specify --json-file path")
            return 1

        latest_dir = extract_dirs[0]

        # Try with invoices first
        json_file = latest_dir / "09_reservations_with_invoices.json"
        if not json_file.exists():
            # Fall back to transformed reservations
            json_file = latest_dir / "03_reservations_transformed.json"

        if not json_file.exists():
            print(f"❌ Reservations file not found in {latest_dir}")
            print("   Please specify --json-file path")
            return 1

        print(f"🎯 Using latest extract: {latest_dir.name}")
        print(f"📄 Using file: {json_file.name}")

    if not json_file.exists():
        print(f"❌ JSON file not found: {json_file}")
        return 1

    print(f"📁 JSON file: {json_file}")
    print(f"📊 Table: {args.table}")
    print(f"🗑️  Truncate: {args.truncate}")
    print(f"📦 Batch size: {args.batch_size}")
    print()

    try:
        count = import_reservations_to_postgres(
            json_file_path=str(json_file),
            table_name=args.table,
            truncate=args.truncate,
            batch_size=args.batch_size,
        )

        print()
        print(f"✅ Successfully imported {count} reservations to {args.table}")
        return 0

    except Exception as e:
        print()
        print(f"❌ Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
