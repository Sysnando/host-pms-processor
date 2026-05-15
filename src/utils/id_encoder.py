"""Helpers for normalizing alphanumeric PMS identifiers into numeric ids
that fit in a SQL BIGINT/Long column."""

import hashlib


def generate_small_id(value: str) -> int:
    """Return a deterministic 12-hex-char prefix of MD5(value) as an int.

    Bounded by 16**12 (~2.8e14), comfortably under signed BIGINT (~9.2e18).
    Used to encode alphanumeric reservation identifiers so they can be
    stored in numeric ``Long`` columns without losing determinism.
    """
    return int(hashlib.md5(value.encode()).hexdigest()[:12], 16)
