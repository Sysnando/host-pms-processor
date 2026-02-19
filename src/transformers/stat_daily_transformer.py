"""Transformer for StatDaily data to update reservation invoice amounts."""

from datetime import datetime
from typing import Any

from structlog import get_logger

from src.models.climber.reservation import ClimberReservation, ReservationCollection
from src.models.host.stat_daily import StatDailyRecord

logger = get_logger(__name__)

# Charge codes to include for revenue calculation
VALID_CHARGE_CODES = ["ALOJ", "NOSHOW", "OB"]


class StatDailyTransformer:
    """Transforms StatDaily data and updates reservation invoice amounts."""

    @staticmethod
    def consolidate_stat_daily_records(
        stat_daily_records: list[StatDailyRecord] | list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Consolidate REVENUE and OCCUPANCY records into single entries.

        Each reservation typically has multiple StatDaily entries per day:
        - HISTORY-REVENUE / FORECAST-REVENUE: Contains revenue data
        - HISTORY-OCCUPANCY / FORECAST-OCCUPANCY: Contains occupancy data and correct HotelDate

        This method:
        1. Groups records by (ResNo, ResId, ChargeCode)
        2. Extracts revenue from revenue records (only ALOJ, NOSHOW, OB)
        3. Uses HotelDate from occupancy record
        4. Returns consolidated records with correct date and revenue

        Args:
            stat_daily_records: List of StatDaily records from API

        Returns:
            List of consolidated records with HotelDate from occupancy and revenue data
        """
        # Convert all records to StatDailyRecord objects
        parsed_records = []
        for record in stat_daily_records:
            if isinstance(record, dict):
                try:
                    record = StatDailyRecord(**record)
                except Exception as e:
                    logger.warning(
                        "Failed to parse StatDaily record",
                        error=str(e),
                    )
                    continue
            parsed_records.append(record)

        # Group records by (ResNo, ResId, ChargeCode)
        groups: dict[tuple[int, int, str], dict[str, StatDailyRecord]] = {}

        for record in parsed_records:
            # Only process ALOJ, NOSHOW, OB charges
            if record.charge_code not in VALID_CHARGE_CODES:
                continue

            key = (record.res_no, record.res_id, record.charge_code)

            if key not in groups:
                groups[key] = {}

            # Store by record type
            groups[key][record.record_type] = record

        # Consolidate groups
        consolidated = []
        for (res_no, res_id, charge_code), records_by_type in groups.items():
            # Try to get revenue and occupancy records (HISTORY or FORECAST)
            revenue_record = (
                records_by_type.get("HISTORY-REVENUE") or
                records_by_type.get("FORECAST-REVENUE")
            )
            occupancy_record = (
                records_by_type.get("HISTORY-OCCUPANCY") or
                records_by_type.get("FORECAST-OCCUPANCY")
            )

            # Use HotelDate from occupancy record if available, otherwise from revenue record
            if occupancy_record:
                hotel_date = occupancy_record.hotel_date
            elif revenue_record:
                hotel_date = revenue_record.hotel_date
            else:
                # No valid records for this group
                continue

            # Get revenue from revenue record
            if revenue_record:
                revenue_net = revenue_record.revenue_net
                global_res_guest_id = revenue_record.global_res_guest_id
            else:
                # No revenue record, skip this group
                logger.debug(
                    "No revenue record found",
                    res_no=res_no,
                    res_id=res_id,
                    charge_code=charge_code,
                )
                continue

            # Create consolidated record
            consolidated.append({
                "hotel_date": hotel_date,
                "res_no": res_no,
                "res_id": res_id,
                "charge_code": charge_code,
                "global_res_guest_id": global_res_guest_id,
                "revenue_net": revenue_net,
            })

        logger.info(
            "Consolidated StatDaily records",
            total_records=len(stat_daily_records),
            consolidated_records=len(consolidated),
            groups_processed=len(groups),
        )

        return consolidated

    @staticmethod
    def aggregate_revenue_by_key(
        consolidated_records: list[dict[str, Any]],
    ) -> tuple[dict[tuple[str, str, int], float], dict[tuple[str, int], float]]:
        """Aggregate RevenueNet with separate handling for NOSHOW charges.

        Regular charges (ALOJ, OB) are keyed by (HotelDate, reservation_id_external, ResId),
        where reservation_id_external is built the same way as in the transformers
        (ResNo + GlobalResGuestId concatenated, no separators).

        NOSHOW charges use only (HotelDate, ResId) since the guest record is removed
        and GlobalResGuestId is not reliable for matching.

        Args:
            consolidated_records: Consolidated records from consolidate_stat_daily_records()

        Returns:
            Tuple of (regular_revenue_map, noshow_revenue_map)
        """
        regular_revenue_map: dict[tuple[str, str, int], float] = {}
        noshow_revenue_map: dict[tuple[str, int], float] = {}

        for record in consolidated_records:
            # Extract date as string (YYYY-MM-DD)
            hotel_date = record["hotel_date"]
            if isinstance(hotel_date, str):
                hotel_date_str = hotel_date.split("T")[0]  # Extract YYYY-MM-DD
            else:
                hotel_date_str = hotel_date.date().isoformat()

            charge_code = record["charge_code"]
            res_no = record["res_no"]
            res_id = record["res_id"]
            global_res_guest_id = record["global_res_guest_id"]
            revenue_net = record["revenue_net"]

            if charge_code == "NOSHOW":
                # NOSHOW: guest record is removed, match only by ResId
                noshow_key = (hotel_date_str, res_id)
                if noshow_key not in noshow_revenue_map:
                    noshow_revenue_map[noshow_key] = 0.0
                noshow_revenue_map[noshow_key] += revenue_net
            else:
                # Regular charges (ALOJ, OB): match by reservation_id_external + ResId
                # reservation_id_external is ResNo+GlobalResGuestId concatenated (no separators)
                reservation_id_external = f"{res_no}{global_res_guest_id}"
                regular_key = (hotel_date_str, reservation_id_external, res_id)
                if regular_key not in regular_revenue_map:
                    regular_revenue_map[regular_key] = 0.0
                regular_revenue_map[regular_key] += revenue_net

        logger.info(
            "Aggregated StatDaily revenue",
            regular_keys=len(regular_revenue_map),
            noshow_keys=len(noshow_revenue_map),
            total_consolidated_records=len(consolidated_records),
        )

        return regular_revenue_map, noshow_revenue_map

    @staticmethod
    def _get_reservation_lookup_key(
        reservation: ClimberReservation,
    ) -> tuple[str, int]:
        """Build lookup key from reservation for matching with StatDaily.

        Args:
            reservation: ClimberReservation object

        Returns:
            Tuple of (reservation_id_external, reservation_id_int)
        """
        # reservation_id_external is the composite key (no separators, stored as Long in DB)
        # reservation_id is the ResId as string
        try:
            res_id = int(reservation.reservation_id)
        except ValueError:
            res_id = 0
            logger.warning(
                "Failed to parse reservation_id as integer",
                reservation_id=reservation.reservation_id,
            )

        return (reservation.reservation_id_external, res_id)

    @staticmethod
    def _build_stat_daily_lookup_key_from_record(
        record: StatDailyRecord,
    ) -> str:
        """Build lookup key from StatDaily record.

        Format: "ResNoGlobalResGuestId" or "ResNoGlobalResGuestIdMasterDetail" (no separators - stored as Long in DB)

        Args:
            record: StatDailyRecord object

        Returns:
            String key matching reservation_id_external format
        """
        if record.master_detail > 0:
            return f"{record.res_no}{record.global_res_guest_id}{record.master_detail}"
        return f"{record.res_no}{record.global_res_guest_id}"

    @staticmethod
    def update_reservation_invoices(
        reservation_collection: ReservationCollection,
        regular_revenue_map: dict[tuple[str, str, int], float],
        noshow_revenue_map: dict[tuple[str, int], float],
    ) -> tuple[ReservationCollection, int, list[dict[str, Any]]]:
        """Update revenue_room_invoice in reservations based on StatDaily data.

        Matches reservations with StatDaily data using:
        - Regular charges (ALOJ, OB): Match by (calendar_date, reservation_id_external, res_id)
        - NOSHOW charges: Match by (calendar_date, res_id) only

        Args:
            reservation_collection: Collection of ClimberReservation objects
            regular_revenue_map: Revenue for ALOJ/OB charges keyed by (calendar_date, reservation_id_external, res_id)
            noshow_revenue_map: Revenue for NOSHOW charges keyed by (calendar_date, res_id)

        Returns:
            Tuple of (updated_collection, update_count, match_details)
        """
        updated_count = 0
        match_details = []

        for reservation in reservation_collection.reservations:
            calendar_date = reservation.calendar_date
            reservation_id_external = reservation.reservation_id_external

            try:
                res_id = int(reservation.reservation_id)
            except ValueError:
                res_id = 0

            matched_revenue = None
            match_type = None

            # Try regular match first (ALOJ, OB)
            regular_key = (calendar_date, reservation_id_external, res_id)
            matched_revenue = regular_revenue_map.get(regular_key)

            if matched_revenue is not None:
                match_type = "regular"
            else:
                # Try NOSHOW match - only by (calendar_date, res_id)
                noshow_key = (calendar_date, res_id)
                matched_revenue = noshow_revenue_map.get(noshow_key)
                if matched_revenue is not None:
                    match_type = "noshow"

            if matched_revenue is not None:
                # Update revenue_room_invoice
                old_value = reservation.revenue_room_invoice
                reservation.revenue_room_invoice = matched_revenue
                updated_count += 1

                match_details.append({
                    "calendar_date": calendar_date,
                    "reservation_id": reservation.reservation_id,
                    "reservation_id_external": reservation_id_external,
                    "old_revenue_room_invoice": old_value,
                    "new_revenue_room_invoice": matched_revenue,
                    "match_type": match_type,
                })

        logger.info(
            "Updated reservation invoices from StatDaily",
            total_reservations=len(reservation_collection.reservations),
            updated_count=updated_count,
            match_rate=f"{(updated_count / len(reservation_collection.reservations) * 100):.1f}%"
            if reservation_collection.reservations
            else "0.0%",
        )

        return reservation_collection, updated_count, match_details

    @staticmethod
    def process_stat_daily_for_reservations(
        stat_daily_records: list[StatDailyRecord] | list[dict[str, Any]],
        reservation_collection: ReservationCollection,
    ) -> tuple[ReservationCollection, dict[str, Any]]:
        """Full pipeline: consolidate, aggregate, and update reservations with StatDaily data.

        Process flow:
        1. Consolidate revenue and occupancy records (HISTORY and FORECAST)
        2. Use HotelDate from occupancy record (corrects edge cases)
        3. Aggregate revenue by key (separate for regular and NOSHOW)
        4. Update reservation invoices

        Args:
            stat_daily_records: Raw StatDaily records from API
            reservation_collection: Collection of transformed reservations

        Returns:
            Tuple of (updated_reservation_collection, processing_stats)
        """
        logger.info(
            "Processing StatDaily data for reservations",
            total_stat_records=len(stat_daily_records),
            total_reservations=len(reservation_collection.reservations),
        )

        # Step 1: Consolidate revenue and occupancy records (HISTORY and FORECAST)
        # This combines multiple record types per day and uses correct HotelDate from occupancy
        consolidated_records = StatDailyTransformer.consolidate_stat_daily_records(
            stat_daily_records
        )

        # Step 2: Aggregate revenue (separate maps for regular and NOSHOW)
        regular_revenue_map, noshow_revenue_map = StatDailyTransformer.aggregate_revenue_by_key(
            consolidated_records
        )

        # Step 3: Update reservations
        updated_collection, updated_count, match_details = (
            StatDailyTransformer.update_reservation_invoices(
                reservation_collection, regular_revenue_map, noshow_revenue_map
            )
        )

        # Compile stats
        stats = {
            "total_stat_records": len(stat_daily_records),
            "consolidated_records_count": len(consolidated_records),
            "consolidated_records": consolidated_records,  # Include full consolidated data
            "regular_aggregated_keys": len(regular_revenue_map),
            "noshow_aggregated_keys": len(noshow_revenue_map),
            "total_reservations": len(reservation_collection.reservations),
            "updated_reservations": updated_count,
            "match_rate": f"{(updated_count / len(reservation_collection.reservations) * 100):.1f}%"
            if reservation_collection.reservations
            else "0.0%",
            "match_details": match_details,
        }

        logger.info(
            "StatDaily processing complete",
            consolidated=len(consolidated_records),
            updated_reservations=updated_count,
            regular_keys=len(regular_revenue_map),
            noshow_keys=len(noshow_revenue_map),
        )

        return updated_collection, stats
