"""Execute stat_daily SQL script against PostgreSQL database."""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def get_db_connection_string():
    """Get database connection string from environment variable or prompt."""
    # Try to get from environment
    db_url = os.environ.get("DATABASE_URL")

    if db_url:
        print(f"✅ Using DATABASE_URL from environment")
        return db_url

    # Otherwise, build from individual components
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "postgres")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "")

    if not db_password:
        print("⚠️  No database password found in environment")
        db_password = input("Enter database password: ")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    print(f"✅ Built connection string: postgresql://{db_user}:****@{db_host}:{db_port}/{db_name}")

    return connection_string


def execute_sql_file(sql_file_path: Path, connection_string: str, create_table: bool = False):
    """Execute SQL file against PostgreSQL database.

    Args:
        sql_file_path: Path to the SQL file
        connection_string: PostgreSQL connection string
        create_table: If True, uncomment and execute the CREATE TABLE statement
    """
    print(f"\n📖 Reading SQL file: {sql_file_path}")

    if not sql_file_path.exists():
        print(f"❌ SQL file not found: {sql_file_path}")
        return False

    # Read SQL file
    with open(sql_file_path, 'r') as f:
        sql_content = f.read()

    print(f"   📊 File size: {len(sql_content) / 1024:.2f} KB")

    # If create_table is True, uncomment the CREATE TABLE statement
    if create_table:
        print("   🔧 Uncommenting CREATE TABLE statement...")
        lines = sql_content.split('\n')
        uncommented_lines = []
        in_create_table = False

        for line in lines:
            if line.strip().startswith("-- CREATE TABLE"):
                in_create_table = True
                uncommented_lines.append(line.replace("-- ", "", 1))
            elif in_create_table:
                if line.strip().startswith("-- );"):
                    uncommented_lines.append(line.replace("-- ", "", 1))
                    in_create_table = False
                elif line.strip().startswith("--"):
                    uncommented_lines.append(line.replace("-- ", "", 1))
                else:
                    uncommented_lines.append(line)
            else:
                uncommented_lines.append(line)

        sql_content = '\n'.join(uncommented_lines)

    # Connect to database
    print(f"\n🔌 Connecting to database...")
    try:
        conn = psycopg2.connect(connection_string)
        conn.autocommit = False
        cursor = conn.cursor()
        print("   ✅ Connected successfully")
    except Exception as e:
        print(f"   ❌ Failed to connect: {str(e)}")
        return False

    try:
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'stat_daily'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print(f"   ℹ️  Table 'stat_daily' already exists")

            # Ask if user wants to truncate
            truncate = input("   ❓ Truncate table before inserting? (y/N): ").strip().lower()
            if truncate == 'y':
                print("   🗑️  Truncating table...")
                cursor.execute("TRUNCATE TABLE stat_daily;")
                print("   ✅ Table truncated")
        else:
            print(f"   ℹ️  Table 'stat_daily' does not exist")
            if not create_table:
                print("   ⚠️  Run with --create-table flag to create the table first")
                return False

        # Execute SQL
        print(f"\n⚙️  Executing SQL script...")
        cursor.execute(sql_content)

        # Get row count
        cursor.execute("SELECT COUNT(*) FROM stat_daily;")
        row_count = cursor.fetchone()[0]

        # Commit transaction
        conn.commit()
        print(f"   ✅ SQL executed successfully")
        print(f"   📊 Total rows in stat_daily table: {row_count}")

        return True

    except Exception as e:
        print(f"   ❌ Error executing SQL: {str(e)}")
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()
        print("   🔌 Database connection closed")


def main():
    parser = argparse.ArgumentParser(
        description="Execute stat_daily SQL script against PostgreSQL database"
    )
    parser.add_argument(
        "--sql-file",
        type=str,
        required=False,
        help="Path to SQL file (if not provided, uses latest from data_extracts)"
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="Create table if it doesn't exist (uncomments CREATE TABLE statement)"
    )
    parser.add_argument(
        "--connection-string",
        type=str,
        required=False,
        help="PostgreSQL connection string (e.g., postgresql://user:pass@localhost:5432/dbname)"
    )

    args = parser.parse_args()

    # Determine SQL file path
    if args.sql_file:
        sql_file = Path(args.sql_file)
    else:
        # Find latest stat_daily_insert.sql in data_extracts
        data_extracts_dir = Path(__file__).parent.parent.parent / "data_extracts"
        extract_dirs = sorted(
            data_extracts_dir.glob("PTLISLSA_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )

        if not extract_dirs:
            print("❌ No data extract directories found")
            return

        latest_dir = extract_dirs[0]
        sql_file = latest_dir / "stat_daily_insert.sql"

        if not sql_file.exists():
            print(f"❌ stat_daily_insert.sql not found in {latest_dir}")
            print("   Run the SQL generator first: python tests/db/stat_daily_sql_generator.py")
            return

        print(f"🎯 Using latest extract: {latest_dir.name}")

    # Get database connection string
    if args.connection_string:
        connection_string = args.connection_string
    else:
        connection_string = get_db_connection_string()

    # Execute SQL
    success = execute_sql_file(sql_file, connection_string, args.create_table)

    if success:
        print(f"\n✨ Done! stat_daily data imported successfully")
    else:
        print(f"\n❌ Failed to import stat_daily data")
        sys.exit(1)


if __name__ == "__main__":
    main()
