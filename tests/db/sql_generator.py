"""SQL INSERT generator for transformed reservations."""

from datetime import datetime
from pathlib import Path
from typing import Any


class SQLGenerator:
    """Generate SQL INSERT statements from transformed reservations."""

    def __init__(self, output_dir: Path):
        """Initialize SQL generator with output directory.

        Args:
            output_dir: Directory to save the SQL script
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_inserts(self, reservations: list[dict[str, Any]]) -> str:
        """Generate SQL INSERT statements from reservation list.

        Args:
            reservations: List of transformed reservation dictionaries

        Returns:
            SQL script as string
        """
        if not reservations:
            print("⚠️  No reservations to generate SQL for")
            return ""

        sql_lines = []

        # Add header comments
        sql_lines.append("-- SQL INSERT script for transformed reservations")
        sql_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        sql_lines.append(f"-- Total records: {len(reservations)}")
        sql_lines.append("")

        # Create table statement (commented out)
        sql_lines.append("-- Create table statement (uncomment if needed):")
        sql_lines.append("""-- CREATE TABLE IF NOT EXISTS reservations2 (
--   record_date VARCHAR(255),
--   calendar_date DATE,
--   calendar_date_start DATE,
--   calendar_date_end DATE,
--   created_date DATE,
--   pax INTEGER,
--   reservation_id INTEGER,
--   reservation_id_external INTEGER,
--   revenue_fb DECIMAL(10, 2),
--   revenue_fb_invoice DECIMAL(10, 2),
--   revenue_others DECIMAL(10, 2),
--   revenue_others_invoice DECIMAL(10, 2),
--   revenue_room DECIMAL(10, 2),
--   revenue_room_invoice DECIMAL(10, 2),
--   rooms INTEGER,
--   status INTEGER,
--   agency_code VARCHAR(255),
--   channel_code VARCHAR(255),
--   company_code VARCHAR(255),
--   cro_code VARCHAR(255),
--   group_code VARCHAR(255),
--   package_code VARCHAR(255),
--   rate_code VARCHAR(255),
--   room_code VARCHAR(255),
--   segment_code VARCHAR(255),
--   sub_segment_code VARCHAR(255)
-- );
""")
        sql_lines.append("")
        sql_lines.append("BEGIN TRANSACTION;")
        sql_lines.append("")

        # Define columns in order
        columns = [
            'record_date', 'calendar_date', 'calendar_date_start', 'calendar_date_end',
            'created_date', 'pax', 'reservation_id', 'reservation_id_external',
            'revenue_fb', 'revenue_fb_invoice', 'revenue_others', 'revenue_others_invoice',
            'revenue_room', 'revenue_room_invoice', 'rooms', 'status',
            'agency_code', 'channel_code', 'company_code', 'cro_code',
            'group_code', 'package_code', 'rate_code', 'room_code',
            'segment_code', 'sub_segment_code'
        ]

        columns_str = ', '.join(columns)

        # Generate one INSERT statement per reservation
        for i, record in enumerate(reservations, 1):
            values = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    values.append('NULL')
                elif isinstance(value, str):
                    # Escape single quotes in strings
                    escaped = value.replace("'", "''")
                    values.append(f"'{escaped}'")
                elif isinstance(value, bool):
                    values.append('TRUE' if value else 'FALSE')
                else:
                    values.append(str(value))

            values_str = ', '.join(values)
            sql_lines.append(f"INSERT INTO reservations2 ({columns_str}) VALUES ({values_str});")

            # Print progress every 1000 records
            if i % 1000 == 0:
                print(f"   ⏳ Generated {i} INSERT statements")

        sql_lines.append("")
        sql_lines.append("COMMIT;")

        sql_script = '\n'.join(sql_lines)
        return sql_script

    def save_script(self, sql_script: str, filename: str = "reservations_insert.sql") -> Path:
        """Save SQL script to file.

        Args:
            sql_script: SQL script content
            filename: Output filename

        Returns:
            Path to the generated file
        """
        output_path = self.output_dir / filename
        with open(output_path, 'w') as f:
            f.write(sql_script)

        return output_path


def generate_sql_from_reservations(
    reservations: list[dict[str, Any]],
    output_dir: Path,
) -> Path:
    """Generate and save SQL INSERT script from transformed reservations.

    Args:
        reservations: List of transformed reservation dictionaries
        output_dir: Directory to save the SQL script

    Returns:
        Path to the generated SQL file
    """
    print("\n💾 Generating SQL INSERT script...")
    print(f"   📊 Reservations to insert: {len(reservations)}")

    generator = SQLGenerator(output_dir)
    sql_script = generator.generate_inserts(reservations)

    if not sql_script:
        return None

    output_file = generator.save_script(sql_script)
    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"   ✅ SQL script generated: {output_file.name}")
    print(f"   📁 File size: {file_size:.2f} MB")
    print(f"   📍 Location: {output_file}")

    return output_file
