"""Tests for alphanumeric reservation_id encoding.

Covers:
- ``src/utils/id_encoder.py::generate_small_id`` unit behavior.
- ``StatDailyRecord`` / ``HostReservation`` accept alphanumeric upstream values.
- ``StatDailyToReservationTransformer`` preserves all-digit ids byte-for-byte
  (no DB backfill needed) and encodes alphanumeric ones to a numeric Long.
"""

import pytest

from src.models.host.reservation import HostReservation
from src.models.host.stat_daily import StatDailyRecord
from src.transformers.stat_daily_to_reservation_transformer import (
    StatDailyToReservationTransformer,
)
from src.utils.id_encoder import generate_small_id


# Signed BIGINT upper bound (Postgres / MySQL).
BIGINT_MAX = 2**63 - 1


def _stat_daily_pair(**overrides) -> list[dict]:
    """Build a minimal occupancy+revenue pair that the transformer accepts."""
    base = {
        "RowNumber": 1,
        "TotalRows": 2,
        "HotelDate": "2026-01-01T00:00:00",
        "ResNo": 12345,
        "ResId": 6365,
        "DetailId": 1,
        "MasterDetail": 0,
        "GlobalResGuestId": 782000,
        "CreatedOn": "2025-12-01T00:00:00",
        "CheckIn": "2026-01-01T00:00:00",
        "CheckOut": "2026-01-02T00:00:00",
        "ResStatus": 1,
        "SalesGroup": 0,
    }
    base.update(overrides)
    occupancy = {
        **base,
        "RecordType": "HISTORY-OCCUPANCY",
        "Category": "STD",
        "RoomNights": 1,
        "Pax": 2,
    }
    revenue = {
        **base,
        "RowNumber": 2,
        "RecordType": "HISTORY-REVENUE",
        "ChargeCode": "ALOJ",
        "RevenueNet": 150.0,
    }
    return [occupancy, revenue]


class TestGenerateSmallId:
    """Direct unit tests for ``generate_small_id``."""

    def test_deterministic(self):
        assert generate_small_id("ABC123") == generate_small_id("ABC123")

    def test_distinct_inputs_distinct_outputs(self):
        assert generate_small_id("A") != generate_small_id("B")
        assert generate_small_id("R6365G782000") != generate_small_id("R6365G782001")

    def test_bounded_by_12_hex_chars(self):
        # 16**12 ≈ 2.8e14 — the upper bound of a 12-hex-char number.
        assert 0 < generate_small_id("ABC123") < 16**12

    def test_fits_signed_bigint(self):
        # Must fit a SQL BIGINT/Long column, which is the whole point.
        assert generate_small_id("some-very-long-alphanumeric-input") < BIGINT_MAX

    def test_handles_unicode(self):
        # MD5 input is bytes; non-ascii input must not crash.
        value = "café-Ω-123"
        assert isinstance(generate_small_id(value), int)


class TestStatDailyRecordAcceptsAlphanumericIds:
    """Pydantic-level type loosening on Host PMS source fields."""

    def test_numeric_int_inputs_are_coerced_to_str(self):
        record = StatDailyRecord.model_validate(_stat_daily_pair()[0])
        assert record.res_id == "6365"
        assert record.res_no == "12345"
        assert record.global_res_guest_id == "782000"

    def test_alphanumeric_inputs_are_accepted(self):
        record = StatDailyRecord.model_validate(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )[0]
        )
        assert record.res_id == "R6365"
        assert record.res_no == "R12345"
        assert record.global_res_guest_id == "G782000"

    def test_master_detail_stays_int(self):
        """``master_detail`` is used in a numeric branch; it must remain int."""
        record = StatDailyRecord.model_validate(_stat_daily_pair(MasterDetail=7)[0])
        assert record.master_detail == 7
        assert isinstance(record.master_detail, int)


class TestHostReservationAcceptsAlphanumericIds:
    """Mirror coverage on the legacy ``HostReservation`` model."""

    def _payload(self, **overrides):
        base = {
            "ResNo": 12345,
            "ResId": 6365,
            "DetailId": 1,
            "MasterDetail": 0,
            "GlobalResGuestId": 782000,
            "CreatedOn": "2025-12-01T00:00:00",
            "LastUpdate": "2025-12-01T00:00:00",
            "CheckIn": "2026-01-01T00:00:00",
            "CheckOut": "2026-01-02T00:00:00",
            "Category": "STD",
            "Agency": "DIRECT",
            "ResStatus": 1,
            "GuestId": 1,
            "Pax": 2,
            "PriceList": "RACK",
            "SegmentDescription": "LEISURE",
            "SubSegmentDescription": "INDIVIDUAL",
            "ChannelDescription": "DIRECT",
        }
        base.update(overrides)
        return base

    def test_numeric_input_coerced_to_str(self):
        r = HostReservation.model_validate(self._payload())
        assert r.res_id == "6365"
        assert r.res_no == "12345"
        assert r.global_res_guest_id == "782000"

    def test_alphanumeric_input_accepted(self):
        r = HostReservation.model_validate(
            self._payload(ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000")
        )
        assert r.res_id == "R6365"
        assert r.res_no == "R12345"
        assert r.global_res_guest_id == "G782000"


class TestReservationIdEncoding:
    """End-to-end behavior on the transformer."""

    def _transform_one(self, records):
        coll = StatDailyToReservationTransformer.transform_batch(
            records, hotel_code="TEST"
        )
        assert len(coll.reservations) == 1, "expected exactly 1 reservation"
        return coll.reservations[0]

    def test_numeric_ids_are_preserved_byte_for_byte(self):
        """All-digit ids must NOT be re-encoded — protects existing DB rows."""
        r = self._transform_one(_stat_daily_pair())
        assert r.reservation_id == "6365782000"
        assert r.reservation_id_external == "12345782000"

    def test_numeric_ids_with_master_detail(self):
        r = self._transform_one(_stat_daily_pair(MasterDetail=7))
        assert r.reservation_id == "63657820007"
        assert r.reservation_id_external == "123457820007"

    def test_alphanumeric_ids_are_encoded_to_digits(self):
        r = self._transform_one(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )
        )
        assert r.reservation_id.isdigit()
        assert r.reservation_id_external.isdigit()

    def test_alphanumeric_encoded_value_matches_helper(self):
        """The encoded id must equal ``generate_small_id`` on the raw concat."""
        r = self._transform_one(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )
        )
        assert r.reservation_id == str(generate_small_id("R6365G782000"))
        assert r.reservation_id_external == str(generate_small_id("R12345G782000"))

    def test_alphanumeric_encoded_value_fits_bigint(self):
        r = self._transform_one(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )
        )
        assert int(r.reservation_id) < BIGINT_MAX
        assert int(r.reservation_id_external) < BIGINT_MAX

    def test_alphanumeric_encoding_is_deterministic_across_runs(self):
        """Same alphanumeric input must always produce the same numeric id —
        otherwise the DB would see duplicates on reprocess."""
        first = self._transform_one(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )
        )
        second = self._transform_one(
            _stat_daily_pair(
                ResNo="R12345", ResId="R6365", GlobalResGuestId="G782000"
            )
        )
        assert first.reservation_id == second.reservation_id
        assert first.reservation_id_external == second.reservation_id_external

    def test_partial_alphanumeric_triggers_encoding(self):
        """Even a single alpha char in any component triggers encoding."""
        r = self._transform_one(_stat_daily_pair(ResId="R6365"))
        # Internal id has alpha → encoded.
        assert r.reservation_id.isdigit()
        assert r.reservation_id == str(generate_small_id("R6365782000"))
        # External id is still all-numeric → preserved.
        assert r.reservation_id_external == "12345782000"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
