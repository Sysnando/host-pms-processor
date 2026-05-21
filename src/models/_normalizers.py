"""Shared pre-validators for Pydantic model fields."""


def _extract_code_str(value):
    """Normalize a code/name field that may arrive as a dict like
    ``{"id": "X", "code": "X", "name": "..."}`` into a flat string.

    Falls back to ``"id"``, then ``str(value)``, to stay forgiving on
    unexpected shapes.
    """
    if isinstance(value, dict):
        return value.get("code") or value.get("id") or str(value)
    return value if isinstance(value, str) else str(value)
