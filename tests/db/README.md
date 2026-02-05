# Database Import Tools (Local Testing Only)

⚠️ **Important:** These database import tools are for **local development and testing only**.

The production application does **not** have database access - it only:
- Fetches data from Host PMS API
- Transforms data to Climber format
- Stores raw and processed data in S3
- Sends notifications to SQS

## Files in this Directory

### Core Importers

- **`postgres_importer.py`** - Import reservations to PostgreSQL (local testing)
- **`stat_daily_importer.py`** - Import stat_daily data to PostgreSQL (local testing)
- **`sql_generator.py`** - Generate SQL INSERT scripts from transformed data

### Usage

These importers are automatically used by `tests/fetch_and_transform_local.py` when available:

```python
# In fetch_and_transform_local.py
try:
    from tests.db.postgres_importer import import_reservations_to_postgres
    from tests.db.stat_daily_importer import import_stat_daily_to_postgres
    DB_IMPORT_AVAILABLE = True
except ImportError:
    DB_IMPORT_AVAILABLE = False
```

## Quick Import Scripts

Use the scripts in `tests/scripts/` for easy importing:

```bash
# Import stat_daily data
python tests/scripts/import_stat_daily.py

# Import reservations
python tests/scripts/import_reservations.py
```

## Production vs Local

| Feature | Production | Local Testing |
|---------|-----------|---------------|
| Database Access | ❌ No | ✅ Yes |
| S3 Storage | ✅ Yes | ✅ Yes |
| SQL Generation | ✅ Yes | ✅ Yes |
| Direct DB Import | ❌ No | ✅ Yes |

## Configuration

Database configuration is in `.env` (local only):

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/reservations
```

This configuration is **not** used in production deployments.
