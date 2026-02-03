"""SQL INSERT generator for stat_daily records."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class StatDailySQLGenerator:
    """Generate SQL INSERT statements for stat_daily records."""

    def __init__(self, output_dir: Path):
        """Initialize SQL generator with output directory.

        Args:
            output_dir: Directory to save the SQL script
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_create_table(self) -> str:
        """Generate CREATE TABLE statement for stat_daily.

        Returns:
            SQL CREATE TABLE statement
        """
        return """CREATE TABLE IF NOT EXISTS stat_daily (
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
);"""

    def _format_value(self, value: Any) -> str:
        """Format a value for SQL insertion.

        Args:
            value: Value to format

        Returns:
            Formatted SQL value string
        """
        if value is None or value == "":
            return 'NULL'
        elif isinstance(value, str):
            # Escape single quotes in strings
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # For any other type, convert to string and quote
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

    def _convert_snake_case(self, camel_case: str) -> str:
        """Convert CamelCase to snake_case.

        Args:
            camel_case: CamelCase string

        Returns:
            snake_case string
        """
        result = []
        for i, char in enumerate(camel_case):
            if char.isupper() and i > 0:
                result.append('_')
            result.append(char.lower())
        return ''.join(result)

    def generate_inserts(self, stat_daily_records: list[dict[str, Any]]) -> str:
        """Generate SQL INSERT statements from stat_daily records.

        Args:
            stat_daily_records: List of stat_daily record dictionaries

        Returns:
            SQL script as string
        """
        if not stat_daily_records:
            print("⚠️  No stat_daily records to generate SQL for")
            return ""

        sql_lines = []

        # Add header comments
        sql_lines.append("-- SQL INSERT script for stat_daily records")
        sql_lines.append(f"-- Generated: {datetime.now().isoformat()}")
        sql_lines.append(f"-- Total records: {len(stat_daily_records)}")
        sql_lines.append("")

        # Add CREATE TABLE statement (commented out)
        sql_lines.append("-- Create table statement (uncomment if needed):")
        for line in self.generate_create_table().split('\n'):
            sql_lines.append(f"-- {line}")
        sql_lines.append("")

        sql_lines.append("BEGIN TRANSACTION;")
        sql_lines.append("")

        # Define columns in order (snake_case)
        columns = [
            'row_number', 'total_rows', 'record_type', 'hotel_date', 'res_no', 'res_id',
            'detail_id', 'master_detail', 'global_res_guest_id', 'created_on',
            'check_in', 'check_out', 'option_date', 'category', 'complex_code',
            'room_name', 'agency', 'company', 'cro', 'groupname', 'res_status',
            'guest_id', 'country_iso_code', 'nationality_iso_code', 'pack',
            'price_list', 'segment_description', 'sub_segment_description',
            'channel_description', 'additional_status_code', 'additional_status_description',
            'category_upgrade_from', 'pax', 'children_type1', 'children_type2',
            'children_type3', 'room_nights', 'charge_code', 'sales_group',
            'sales_group_desc', 'revenue_gross', 'revenue_net'
        ]

        # Map CamelCase keys to snake_case
        camel_to_snake = {
            'RowNumber': 'row_number',
            'TotalRows': 'total_rows',
            'RecordType': 'record_type',
            'HotelDate': 'hotel_date',
            'ResNo': 'res_no',
            'ResId': 'res_id',
            'DetailId': 'detail_id',
            'MasterDetail': 'master_detail',
            'GlobalResGuestId': 'global_res_guest_id',
            'CreatedOn': 'created_on',
            'CheckIn': 'check_in',
            'CheckOut': 'check_out',
            'OptionDate': 'option_date',
            'Category': 'category',
            'ComplexCode': 'complex_code',
            'RoomName': 'room_name',
            'Agency': 'agency',
            'Company': 'company',
            'Cro': 'cro',
            'Groupname': 'groupname',
            'ResStatus': 'res_status',
            'Guest_Id': 'guest_id',
            'CountryIsoCode': 'country_iso_code',
            'NationalityIsoCode': 'nationality_iso_code',
            'Pack': 'pack',
            'PriceList': 'price_list',
            'SegmentDescription': 'segment_description',
            'SubSegmentDescription': 'sub_segment_description',
            'ChannelDescription': 'channel_description',
            'AdditionalStatusCode': 'additional_status_code',
            'AdditionalStatusDescription': 'additional_status_description',
            'CategoryUpgradeFrom': 'category_upgrade_from',
            'Pax': 'pax',
            'ChildrenType1': 'children_type1',
            'ChildrenType2': 'children_type2',
            'ChildrenType3': 'children_type3',
            'RoomNights': 'room_nights',
            'ChargeCode': 'charge_code',
            'SalesGroup': 'sales_group',
            'SalesGroupDesc': 'sales_group_desc',
            'RevenueGross': 'revenue_gross',
            'RevenueNet': 'revenue_net'
        }

        columns_str = ', '.join(columns)

        # Generate one INSERT statement per record
        for i, record in enumerate(stat_daily_records, 1):
            values = []
            for col in columns:
                # Find the corresponding CamelCase key
                camel_key = next((k for k, v in camel_to_snake.items() if v == col), None)
                value = record.get(camel_key) if camel_key else None
                values.append(self._format_value(value))

            values_str = ', '.join(values)
            sql_lines.append(f"INSERT INTO stat_daily ({columns_str}) VALUES ({values_str});")

            # Print progress every 100 records
            if i % 100 == 0:
                print(f"   ⏳ Generated {i} INSERT statements")

        sql_lines.append("")
        sql_lines.append("COMMIT;")

        sql_script = '\n'.join(sql_lines)
        return sql_script

    def save_script(self, sql_script: str, filename: str = "stat_daily_insert.sql") -> Path:
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


def generate_sql_from_stat_daily(
    json_file_path: Path,
    output_dir: Path,
) -> Path:
    """Generate and save SQL INSERT script from stat_daily JSON file.

    Args:
        json_file_path: Path to the stat_daily_raw.json file
        output_dir: Directory to save the SQL script

    Returns:
        Path to the generated SQL file
    """
    print(f"\n📖 Reading stat_daily data from: {json_file_path}")

    # Load JSON data
    with open(json_file_path, 'r') as f:
        stat_daily_records = json.load(f)

    print(f"   📊 Records loaded: {len(stat_daily_records)}")

    print("\n💾 Generating SQL INSERT script...")

    generator = StatDailySQLGenerator(output_dir)
    sql_script = generator.generate_inserts(stat_daily_records)

    if not sql_script:
        return None

    output_file = generator.save_script(sql_script)
    file_size = output_file.stat().st_size / 1024

    print(f"   ✅ SQL script generated: {output_file.name}")
    print(f"   📁 File size: {file_size:.2f} KB")
    print(f"   📍 Location: {output_file}")

    return output_file


if __name__ == "__main__":
    """Example usage: Generate SQL from the latest stat_daily_raw.json file."""
    from pathlib import Path

    # Find the latest data extract directory
    data_extracts_dir = Path(__file__).parent.parent.parent / "data_extracts"

    # Get all extract directories (sorted by modification time, most recent first)
    extract_dirs = sorted(data_extracts_dir.glob("PTLISLSA_*"), key=lambda p: p.stat().st_mtime, reverse=True)

    if not extract_dirs:
        print("❌ No data extract directories found")
        exit(1)

    latest_dir = extract_dirs[0]
    json_file = latest_dir / "08_stat_daily_raw.json"

    if not json_file.exists():
        print(f"❌ stat_daily_raw.json not found in {latest_dir}")
        exit(1)

    print(f"🎯 Using latest extract: {latest_dir.name}")

    # Generate SQL script in the same directory
    output_path = generate_sql_from_stat_daily(json_file, latest_dir)

    if output_path:
        print(f"\n✨ Done! SQL script ready at: {output_path}")
    else:
        print("\n❌ Failed to generate SQL script")
