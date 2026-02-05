"""Transformer to convert StatDaily data directly to Climber Reservation format.

This transformer creates reservations from StatDaily records without requiring
data from the /reservation endpoint. StatDaily contains all necessary information:
occupancy, revenue, segments, dates, pax, etc.
"""

from datetime import datetime
from typing import Any, Optional

from structlog import get_logger

from src.models.climber.reservation import ClimberReservation, ReservationCollection
from src.models.host.stat_daily import StatDailyRecord

logger = get_logger(__name__)

# Charge code configuration
ROOM_CHARGE_CODES = ["ALOJ", "OB"]  # Room revenue (ALOJ=accommodation, OB=overbooking)
OTHER_CHARGE_CODES = ["TXCANCEL"]  # Other charges to include
NOSHOW_CHARGE_CODES = ["NOSHOW"]  # No-show charges
EXCLUDED_CHARGE_CODES = ["PA", "PAEXTRA", "BARBEB13"]  # F&B - explicitly excluded

# Status mapping from Host PMS ResStatus to Climber status codes
# Climber: 0=CANCELLED, 1=CHECKED_IN, 2=CHECKED_OUT, 3=CONFIRMED, 4=NO_SHOW, 5=TENTATIVE
STATUS_MAP = {
    0: 3,   # STANDARD → CONFIRMED
    2: 5,   # OPTION → TENTATIVE
    3: 5,   # WAITLIST → TENTATIVE
    5: 0,   # OOO → CANCELLED
    6: 0,   # CXL → CANCELLED
    7: 4,   # NOSHOW → NO_SHOW
    8: 0,   # OOI → CANCELLED
    10: 1,  # CI → CHECKED_IN
    20: 2,  # CO → CHECKED_OUT
}


class StatDailyToReservationTransformer:
    """Transforms StatDaily data directly into Climber reservation format."""

    @staticmethod
    def _parse_stat_daily_records(
        stat_daily_records: list[StatDailyRecord] | list[dict[str, Any]],
    ) -> list[StatDailyRecord]:
        """Parse StatDaily records into StatDailyRecord objects.

        Args:
            stat_daily_records: List of StatDaily records (dict or StatDailyRecord)

        Returns:
            List of parsed StatDailyRecord objects
        """
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
        return parsed_records

    @staticmethod
    def _group_stat_daily_by_reservation_day(
        stat_daily_records: list[StatDailyRecord],
    ) -> dict[tuple[int, int, int, str], list[StatDailyRecord]]:
        """Group StatDaily records by (ResNo, GlobalResGuestId, ResId, HotelDate).

        Each group represents a unique reservation-day combination.

        Args:
            stat_daily_records: List of parsed StatDaily records

        Returns:
            Dictionary mapping (res_no, global_res_guest_id, res_id, hotel_date) to list of records
        """
        groups: dict[tuple[int, int, int, str], list[StatDailyRecord]] = {}

        for record in stat_daily_records:
            # Extract hotel_date as string (YYYY-MM-DD)
            if isinstance(record.hotel_date, str):
                hotel_date_str = record.hotel_date.split("T")[0]
            else:
                hotel_date_str = record.hotel_date.date().isoformat()

            # Create composite key
            key = (
                record.res_no,
                record.global_res_guest_id,
                record.res_id,
                hotel_date_str,
            )

            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        logger.info(
            "Grouped StatDaily records by reservation-day",
            total_records=len(stat_daily_records),
            unique_groups=len(groups),
        )

        return groups

    @staticmethod
    def _extract_date_string(dt: datetime | str) -> str:
        """Extract date string in YYYY-MM-DD format.

        Args:
            dt: datetime object or ISO string

        Returns:
            Date string in YYYY-MM-DD format
        """
        if isinstance(dt, str):
            return dt.split("T")[0]
        return dt.date().isoformat()

    @staticmethod
    def _get_segment_code(value: Optional[str], default: str = "UNASSIGNED") -> str:
        """Get segment code with default fallback.

        Args:
            value: Segment code value from StatDaily
            default: Default value if None or empty

        Returns:
            Segment code or default
        """
        if value and value.strip():
            return value.strip()
        return default

    @staticmethod
    def _extract_package_code(pack: Optional[str]) -> str:
        """Extract package code from Pack field.

        Pack format: "AP|RO" or "APA|BB" - extract first part before |

        Args:
            pack: Pack field from StatDaily

        Returns:
            Package code or "UNASSIGNED"
        """
        if pack and pack.strip():
            # Take first part before |
            parts = pack.split("|")
            if parts:
                return parts[0].strip()
        return "UNASSIGNED"

    @staticmethod
    def _transform_group_to_reservation(
        records: list[StatDailyRecord],
        hotel_code: str,
        hotel_local_time: Optional[datetime] = None,
    ) -> Optional[ClimberReservation]:
        """Transform a group of StatDaily records into a single ClimberReservation.

        Args:
            records: List of StatDaily records for same reservation-day
            hotel_code: Hotel code
            hotel_local_time: Hotel local time for record_date calculation

        Returns:
            ClimberReservation object or None if transformation fails
        """
        if not records:
            return None

        # Find ALL HISTORY-OCCUPANCY records and HISTORY-REVENUE records
        # Multiple HISTORY-OCCUPANCY records can exist for the same reservation-day
        occupancy_records = []
        revenue_records = []

        for record in records:
            if record.record_type == "HISTORY-OCCUPANCY":
                occupancy_records.append(record)
            elif record.record_type == "HISTORY-REVENUE":
                # Only include valid charge codes
                if record.charge_code in (
                    ROOM_CHARGE_CODES + OTHER_CHARGE_CODES + NOSHOW_CHARGE_CODES
                ):
                    revenue_records.append(record)

        # Validate: each group should have at least one HISTORY-OCCUPANCY record
        if not occupancy_records:
            logger.warning(
                "Group missing HISTORY-OCCUPANCY record",
                hotel_code=hotel_code,
                res_no=records[0].res_no,
                res_id=records[0].res_id,
                hotel_date=records[0].hotel_date,
                record_types=[r.record_type for r in records],
            )

        # Use first occupancy record as base, or first revenue record if no occupancy
        base_record = occupancy_records[0] if occupancy_records else (
            revenue_records[0] if revenue_records else records[0]
        )

        # Extract dates
        hotel_date = StatDailyToReservationTransformer._extract_date_string(
            base_record.hotel_date
        )
        check_in = StatDailyToReservationTransformer._extract_date_string(
            base_record.check_in
        )
        check_out = StatDailyToReservationTransformer._extract_date_string(
            base_record.check_out
        )
        created_date = StatDailyToReservationTransformer._extract_date_string(
            base_record.created_on
        )

        # Calculate record_date (PostgreSQL date range format)
        if hotel_local_time:
            record_date_str = hotel_local_time.date().isoformat()
        else:
            record_date_str = datetime.now().date().isoformat()
        record_date = f"[{record_date_str},)"

        # Build reservation_id_external (composite key)
        if base_record.master_detail > 0:
            reservation_id_external = (
                f"{base_record.res_no}-{base_record.global_res_guest_id}-{base_record.master_detail}"
            )
        else:
            reservation_id_external = (
                f"{base_record.res_no}-{base_record.global_res_guest_id}"
            )

        # Get occupancy data from HISTORY-OCCUPANCY records
        # IMPORTANT: Sum room_nights and pax from ALL occupancy records
        # Multiple HISTORY-OCCUPANCY records can exist for the same reservation-day
        if occupancy_records:
            # Sum room_nights from all occupancy records
            rooms = sum(occ.room_nights if occ.room_nights else 0 for occ in occupancy_records)
            # Sum pax from all occupancy records
            pax = sum(occ.pax if occ.pax else 0 for occ in occupancy_records)
            # Use category from first occupancy record
            room_code = StatDailyToReservationTransformer._get_segment_code(
                occupancy_records[0].category
            )
        else:
            # Fallback: if no occupancy record exists (revenue-only)
            rooms = 0
            pax = 0
            room_code = "UNASSIGNED"

        # Aggregate revenue from HISTORY-REVENUE records
        revenue_room = 0.0
        revenue_others = 0.0
        is_noshow = False

        for rev_record in revenue_records:
            revenue_net = rev_record.revenue_net or 0.0

            if rev_record.charge_code in ROOM_CHARGE_CODES:
                revenue_room += revenue_net
            elif rev_record.charge_code in NOSHOW_CHARGE_CODES:
                revenue_room += revenue_net  # NOSHOW revenue goes to room
                is_noshow = True
            elif rev_record.charge_code in OTHER_CHARGE_CODES:
                revenue_others += revenue_net

        # Map status
        res_status = base_record.res_status
        if is_noshow:
            status = 4  # NO_SHOW
        else:
            status = STATUS_MAP.get(res_status, 3)  # Default to CONFIRMED

        # Override rooms to 0 for cancelled reservations (but keep revenue)
        if res_status == 6:  # CXL status
            rooms = 0

        # Extract segments
        agency_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.agency
        )
        channel_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.channel_description
        )
        company_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.company
        )
        cro_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.cro
        )
        group_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.groupname
        )
        package_code = StatDailyToReservationTransformer._extract_package_code(
            base_record.pack
        )
        rate_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.price_list
        )
        segment_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.segment_description
        )
        sub_segment_code = StatDailyToReservationTransformer._get_segment_code(
            base_record.sub_segment_description
        )

        # Create ClimberReservation
        try:
            reservation = ClimberReservation(
                record_date=record_date,
                calendar_date=hotel_date,
                calendar_date_start=check_in,
                calendar_date_end=check_out,
                created_date=created_date,
                pax=pax,
                reservation_id=str(base_record.res_id),
                reservation_id_external=reservation_id_external,
                revenue_fb=0.0,  # Not extracting from StatDaily
                revenue_fb_invoice=0.0,
                revenue_others=revenue_others,
                revenue_others_invoice=revenue_others,  # StatDaily is historical/invoiced
                revenue_room=revenue_room,
                revenue_room_invoice=revenue_room,  # StatDaily is historical/invoiced
                rooms=rooms,
                status=status,
                agency_code=agency_code,
                channel_code=channel_code,
                company_code=company_code,
                cro_code=cro_code,
                group_code=group_code,
                package_code=package_code,
                rate_code=rate_code,
                room_code=room_code,
                segment_code=segment_code,
                sub_segment_code=sub_segment_code,
            )
            return reservation

        except Exception as e:
            logger.error(
                "Failed to create ClimberReservation from StatDaily group",
                hotel_code=hotel_code,
                res_no=base_record.res_no,
                res_id=base_record.res_id,
                hotel_date=hotel_date,
                error=str(e),
            )
            return None

    @staticmethod
    def transform_batch(
        stat_daily_records: list[StatDailyRecord] | list[dict[str, Any]],
        hotel_code: str,
        hotel_local_time: Optional[datetime] = None,
    ) -> ReservationCollection:
        """Transform batch of StatDaily records into ReservationCollection.

        Process flow:
        1. Parse StatDaily records into StatDailyRecord objects
        2. Group by (ResNo, GlobalResGuestId, ResId, HotelDate)
        3. Transform each group into ClimberReservation
        4. Return ReservationCollection

        Args:
            stat_daily_records: List of StatDaily records from API
            hotel_code: Hotel code
            hotel_local_time: Hotel local time for record_date calculation

        Returns:
            ReservationCollection with transformed reservations
        """
        logger.info(
            "Starting StatDaily to Reservation transformation",
            hotel_code=hotel_code,
            total_records=len(stat_daily_records),
        )

        # Step 1: Parse records
        parsed_records = StatDailyToReservationTransformer._parse_stat_daily_records(
            stat_daily_records
        )

        if not parsed_records:
            logger.warning(
                "No valid StatDaily records to transform",
                hotel_code=hotel_code,
            )
            return ReservationCollection(reservations=[])

        # Step 2: Group by reservation-day
        groups = StatDailyToReservationTransformer._group_stat_daily_by_reservation_day(
            parsed_records
        )

        # Step 3: Transform each group
        reservations = []
        failed_count = 0

        for group_key, group_records in groups.items():
            reservation = StatDailyToReservationTransformer._transform_group_to_reservation(
                group_records,
                hotel_code=hotel_code,
                hotel_local_time=hotel_local_time,
            )

            if reservation:
                reservations.append(reservation)
            else:
                failed_count += 1

        logger.info(
            "StatDaily to Reservation transformation complete",
            hotel_code=hotel_code,
            total_stat_records=len(stat_daily_records),
            unique_groups=len(groups),
            reservations_created=len(reservations),
            failed_transformations=failed_count,
        )

        return ReservationCollection(reservations=reservations)
