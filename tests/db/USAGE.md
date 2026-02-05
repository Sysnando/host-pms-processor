# Database Import Usage (Local Testing Only)

⚠️ **This directory contains tools for LOCAL TESTING ONLY - not for production use.**

## Quick Start

### 1. Set Database URL

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/reservations"
```

### 2. Import Data

**Import stat_daily:**
```bash
python3 tests/scripts/import_stat_daily.py
```

**Import reservations:**
```bash
python3 tests/scripts/import_reservations.py
```

## Success Example

```bash
$ export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/reservations"
$ python3 tests/scripts/import_stat_daily.py

🎯 Using latest extract: PTLISLSA_20260203_084352
📁 JSON file: .../data_extracts/PTLISLSA_20260203_084352/08_stat_daily_raw.json
📊 Table: stat_daily
🗑️  Truncate: False
📦 Batch size: 1000

✅ Successfully imported 3474 stat_daily records to stat_daily
```

## Production vs Local

| Aspect | Production | Local Testing |
|--------|-----------|---------------|
| Location | AWS Lambda/ECS | Your machine |
| Database | ❌ No access | ✅ PostgreSQL |
| S3 Storage | ✅ Yes | Optional |
| SQL Generation | ✅ Yes | ✅ Yes |
| Purpose | Production ETL | Development/Testing |

## Files in This Directory

- **`postgres_importer.py`** - Reservations importer
- **`stat_daily_importer.py`** - StatDaily importer
- **`sql_generator.py`** - SQL script generator
- **`stat_daily_sql_generator.py`** - StatDaily SQL generator
- **`execute_stat_daily_sql.py`** - Execute SQL scripts

## Command Reference

```bash
# Import latest data
python3 tests/scripts/import_stat_daily.py
python3 tests/scripts/import_reservations.py

# Import specific file
python3 tests/scripts/import_stat_daily.py --json-file data_extracts/HOTEL_20260203/08_stat_daily_raw.json

# Truncate before import
python3 tests/scripts/import_stat_daily.py --truncate

# Custom table name
python3 tests/scripts/import_stat_daily.py --table my_stat_daily

# Custom batch size
python3 tests/scripts/import_stat_daily.py --batch-size 2000
```

## Environment Setup

The DATABASE_URL is NOT in `.env` to avoid conflicts with production settings.

Always set it as an environment variable before running import scripts:

```bash
export DATABASE_URL="postgresql://user:pass@host:port/database"
```

Or inline:

```bash
DATABASE_URL="postgresql://user:pass@host:port/database" python3 tests/scripts/import_stat_daily.py
```
