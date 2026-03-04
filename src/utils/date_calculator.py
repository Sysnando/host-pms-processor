"""Utilities for calculating date ranges based on ESB import parameters."""

from datetime import datetime, timedelta


def calculate_date_ranges(
    last_import_date: str = None,
    min_import_date: str = None,
    max_import_date: str = None,
) -> tuple[str, str, str, str, str]:
    """Calculate date ranges for data import based on ESB parameters.

    Args:
        last_import_date: Last import date from ESB (ISO format with time: "2024-01-15T10:30:45Z")
                         None if this is the first import
        min_import_date: Minimum allowed import date from ESB (ISO format: "2022-01-01T00:00:00Z")
        max_import_date: Maximum allowed import date from ESB (ISO format: "2026-12-31T23:59:59Z")

    Returns:
        Tuple of (reservation_from_date, stat_daily_start_date, stat_daily_end_date,
                  inventory_from_date, inventory_to_date)

    Logic:
        - If lastImportDate is null: Import 2 years past and min(2 years future, maxImportDate)
        - If lastImportDate exists: Import from (lastImportDate - 7 days) to min(today + 720 days, maxImportDate)
    """
    today = datetime.now().date()

    if last_import_date is None or last_import_date == "":
        # First time import: 2 years in the past, min(2 years future, maxImportDate)
        from_date = (today - timedelta(days=730)).isoformat()  # 2 years ago

        # Calculate default to_date: 2 years in the future
        default_to_date = today + timedelta(days=730)

        # Use the smaller date between default and maxImportDate
        if max_import_date:
            max_import_dt = datetime.fromisoformat(max_import_date.replace('Z', '+00:00')).date()
            to_date = min(default_to_date, max_import_dt).isoformat()
        else:
            to_date = default_to_date.isoformat()

        # For reservations, use ISO datetime format
        reservation_from_date = f"{from_date}T00:00:00Z"

        # StatDaily uses date-only format
        stat_daily_start = from_date
        stat_daily_end = to_date

        # Inventory uses date-only format
        inventory_from = from_date
        inventory_to = to_date
    else:
        # Incremental import: lastImportDate - 7 days to min(today + 720 days, maxImportDate)
        # Parse lastImportDate (format: "2024-01-15T10:30:45Z")
        last_import_dt = datetime.fromisoformat(last_import_date.replace('Z', '+00:00'))

        # Calculate from_date: lastImportDate - 7 days
        from_date = (last_import_dt.date() - timedelta(days=7)).isoformat()

        # Calculate default to_date: today + 720 days (2 years future)
        default_to_date = today + timedelta(days=720)

        # Use the smaller date between default and maxImportDate
        if max_import_date:
            max_import_dt = datetime.fromisoformat(max_import_date.replace('Z', '+00:00')).date()
            to_date = min(default_to_date, max_import_dt).isoformat()
        else:
            to_date = default_to_date.isoformat()

        # For reservations, use the lastImportDate as-is (with time component)
        reservation_from_date = last_import_date

        # StatDaily uses date-only format
        stat_daily_start = from_date
        stat_daily_end = to_date

        # Inventory uses date-only format
        inventory_from = from_date
        inventory_to = to_date

    return (
        reservation_from_date,
        stat_daily_start,
        stat_daily_end,
        inventory_from,
        inventory_to,
    )
