"""Transformer for converting Host PMS reservations to Climber standardized format."""

from datetime import datetime, timedelta
from typing import Any

from structlog import get_logger

from src.models.climber.reservation import (
    ClimberReservation,
    ReservationCollection,
)
from src.models.host.reservation import HostReservation
from src.models.reservation_status import ReservationStatus, ReservationStatusMapper

logger = get_logger(__name__)


class ReservationTransformer:
    """Transforms Host PMS API reservations to Climber standardized format."""

    @staticmethod
    def _get_status_code(res_status_int: int, reservation_status_map: dict[int, str], hotel_code: str = "UNKNOWN") -> int:
        """Get Climber status code from Host PMS reservation status integer.

        Maps Host PMS ConfigId (integer) to status code string, then to Climber format (0-5).
        Uses provided status mappings from config, with fallback to default mapping.

        Args:
            res_status_int: Status integer from Host PMS API (e.g., 10 for "CI", 20 for "CO")
            reservation_status_map: Dictionary mapping ConfigId integers to status code strings
            hotel_code: Hotel code for logging context

        Returns:
            Integer status code (0-5) for Climber format
        """
        # First try to look up the status code string from config mapping
        if res_status_int in reservation_status_map:
            status_code_str = reservation_status_map[res_status_int]
        else:
            # If not found in config mapping, use a default or log warning
            status_code_str = None
            logger.warning(
                "Status code not found in config mapping",
                hotel_code=hotel_code,
                res_status_int=res_status_int,
            )

        # Map the status code string to Climber status integer
        if status_code_str:
            climber_status = ReservationStatusMapper.map_host_status_to_climber(status_code_str)
        else:
            # Default to CONFIRMED if we can't determine the status
            from src.models.reservation_status import ReservationStatus
            climber_status = ReservationStatus.CONFIRMED

        # logger.debug(
        #     "Status code mapping",
        #     host_status_int=res_status_int,
        #     host_status_code=status_code_str,
        #     climber_status_code=climber_status,
        # )

        return climber_status

    @staticmethod
    def _get_date_string(date_input: datetime | str) -> str:
        """Convert date input to ISO format string (YYYY-MM-DD).

        Args:
            date_input: datetime or string date

        Returns:
            ISO format date string
        """
        if isinstance(date_input, str):
            # Parse ISO string and extract date part
            dt = datetime.fromisoformat(date_input.replace("Z", "+00:00"))
            return dt.date().isoformat()
        else:
            # It's already a datetime
            return date_input.date().isoformat()

    @staticmethod
    def _calculate_revenues(
        reservation: HostReservation,
    ) -> tuple[float, float, float]:
        """Calculate room, F&B, and other revenues for a reservation (IVA removed).

        Only includes revenue if status affects occupancy (1-3).
        Otherwise all revenues are zero.

        IVA is removed based on sales group:
        - Sales group 0 (room): 6% IVA
        - Sales group 1 (F&B): 13% IVA
        - Sales group > 1 (others): 23% IVA

        Args:
            reservation: Host PMS reservation with pricing details

        Returns:
            Tuple of (room_revenue, fb_revenue, other_revenue) as floats (without IVA)
        """
        # Only count revenue for statuses that affect occupancy/revenue
        if reservation.res_status not in REVENUE_AFFECTING_STATUSES:
            return 0.0, 0.0, 0.0

        # Calculate revenues from prices array, grouped by sales group, removing IVA
        room_revenue = 0.0
        fb_revenue = 0.0
        other_revenue = 0.0

        for price_item in reservation.prices:
            if price_item.sales_group == 0:
                # Room revenue: 6% IVA
                room_revenue += price_item.amount / 1.06
            elif price_item.sales_group == 1:
                # F&B revenue: 13% IVA
                fb_revenue += price_item.amount / 1.13
            else:
                # Other revenue: 23% IVA
                other_revenue += price_item.amount / 1.23

        return room_revenue, fb_revenue, other_revenue

    @staticmethod
    def _calculate_revenues_for_date(
        reservation: HostReservation,
        target_date: datetime,
    ) -> tuple[float, float, float]:
        """Calculate room, F&B, and other revenues for a specific stay date (IVA removed).

        Maps revenues from the Prices array by matching the price date with target date.
        Revenues are included regardless of reservation status.

        IVA is removed based on sales group:
        - Sales group 0 (room): 6% IVA
        - Sales group 1 (F&B): 13% IVA
        - Sales group > 1 (others): 23% IVA

        Mapping rules:
        - Prices[].SalesGroup = 0 => revenue_room
        - Prices[].SalesGroup = 1 => revenue_fb
        - Prices[].SalesGroup > 1 => revenue_others

        Args:
            reservation: Host PMS reservation with pricing details
            target_date: The specific date to calculate revenues for

        Returns:
            Tuple of (room_revenue, fb_revenue, other_revenue) as floats for that date (without IVA)
        """
        # Extract date from target_date
        target_date_str = (
            target_date.date().isoformat()
            if isinstance(target_date, datetime)
            else target_date
        )

        # Calculate revenues from prices array for this date, grouped by sales group, removing IVA
        room_revenue = 0.0
        fb_revenue = 0.0
        other_revenue = 0.0

        for price_item in reservation.prices:
            price_date_str = ReservationTransformer._get_date_string(price_item.date)
            if price_date_str == target_date_str:
                if price_item.sales_group == 0:
                    # Room revenue: 6% IVA
                    room_revenue += price_item.amount / 1.06
                elif price_item.sales_group == 1:
                    # F&B revenue: 13% IVA
                    fb_revenue += price_item.amount / 1.13
                else:
                    # Other revenue: 23% IVA
                    other_revenue += price_item.amount / 1.23

        return room_revenue, fb_revenue, other_revenue

    @staticmethod
    def transform(
        reservation: HostReservation | dict[str, Any],
        reservation_status_map: dict[int, str] | None = None,
        hotel_local_time: datetime | str | None = None,
        hotel_code: str = "UNKNOWN",
    ) -> list[ClimberReservation]:
        """Transform a single Host PMS reservation to Climber format using Prices array.

        Creates one record per unique calendar_date found in the Prices array.
        Revenues are calculated from the Prices array grouped by date.
        Does NOT use CheckIn/CheckOut dates for calculation.

        reservation_id_external format:
        - If MasterDetail > 0: "{ResNo}-{GlobalResGuestId}-{MasterDetail}"
        - If MasterDetail == 0: "{ResNo}-{GlobalResGuestId}"

        Args:
            reservation: Reservation from Host PMS API
            reservation_status_map: Optional dictionary mapping ConfigId integers to status code strings
            hotel_local_time: Optional hotel local time from HotelInfo for determining record_date start
            hotel_code: Hotel code for logging context

        Returns:
            List of ClimberReservations in Climber standardized format (one per price date)
        """
        if reservation_status_map is None:
            reservation_status_map = {}
        # Convert dict to HostReservation model if needed
        if isinstance(reservation, dict):
            try:
                reservation = HostReservation(**reservation)
            except Exception as e:
                logger.error(
                    "Failed to parse host reservation",
                    hotel_code=hotel_code,
                    error=str(e),
                )
                raise ValueError(f"Invalid reservation format: {str(e)}") from e

        # If no prices, return empty list
        if not reservation.prices:
            return []

        # Extract created date
        created_date = ReservationTransformer._get_date_string(reservation.created_on)

        # Group prices by date to get unique calendar dates
        prices_by_date: dict[str, list[Any]] = {}
        for price_item in reservation.prices:
            price_date_str = ReservationTransformer._get_date_string(price_item.date)
            if price_date_str not in prices_by_date:
                prices_by_date[price_date_str] = []
            prices_by_date[price_date_str].append(price_item)

        # Get earliest and latest dates from prices for calendar_date_start/end
        all_dates = sorted(prices_by_date.keys())
        if not all_dates:
            return []

        calendar_date_start = all_dates[0]
        calendar_date_end = all_dates[-1]

        # Determine record_date start
        record_date_start = calendar_date_start
        if hotel_local_time is not None:
            hotel_time_date = ReservationTransformer._get_date_string(hotel_local_time)
            if calendar_date_end < hotel_time_date:
                record_date_start = calendar_date_end
            else:
                record_date_start = hotel_time_date

        # Record date is an open-ended date range
        record_date = f"[{record_date_start},)"

        # Get status code as integer (0-5) using the config mapping
        status = ReservationTransformer._get_status_code(reservation.res_status, reservation_status_map, hotel_code)

        # Build segment codes with "UNASSIGNED" as default
        agency_code = reservation.agency or "UNASSIGNED"
        channel_code = reservation.channel_description or "UNASSIGNED"
        company_code = reservation.company or "UNASSIGNED"
        cro_code = "UNASSIGNED"  # Not provided by Host PMS
        group_code = reservation.group_name or "UNASSIGNED"
        package_code = reservation.pack or "UNASSIGNED"
        rate_code = reservation.price_list or "UNASSIGNED"
        room_code = reservation.category or "UNASSIGNED"
        segment_code = reservation.segment_description or "UNASSIGNED"
        sub_segment_code = (
            reservation.sub_segment_description or "UNASSIGNED"
        )

        # Use ResNo-GlobalResGuestId as reservation_id_external
        # If MasterDetail > 0, include it to differentiate multi-detail reservations
        if reservation.master_detail > 0:
            reservation_id_external = f"{reservation.res_no}-{reservation.global_res_guest_id}-{reservation.master_detail}"
        else:
            reservation_id_external = f"{reservation.res_no}-{reservation.global_res_guest_id}"

        # Get checkout date for comparison
        checkout_date_str = ReservationTransformer._get_date_string(reservation.check_out)

        # Create one Climber reservation per unique date in Prices
        climber_reservations = []

        for calendar_date, price_items in prices_by_date.items():
            # Calculate revenues for this date from all price items
            room_revenue = 0.0
            fb_revenue = 0.0
            other_revenue = 0.0

            for price_item in price_items:
                if price_item.sales_group == 0:
                    # Room revenue: 6% IVA
                    room_revenue += price_item.amount / 1.06
                elif price_item.sales_group == 1:
                    # F&B revenue: 13% IVA
                    fb_revenue += price_item.amount / 1.13
                else:
                    # Other revenue: 23% IVA
                    other_revenue += price_item.amount / 1.23

            # Set rooms to 0 in the following cases:
            # 1. Price date >= checkout (post-checkout charges)
            # 2. Reservation status is NO_SHOW (status=4)
            # In both cases, revenue is still included
            is_no_show = status == ReservationStatus.NO_SHOW
            is_post_checkout = calendar_date >= checkout_date_str
            rooms_count = 0 if (is_post_checkout or is_no_show) else reservation.rooms

            # Set revenue_room_invoice to 0 for no-shows
            # It will be adjusted later by StatDaily information
            room_revenue_invoice = 0.0 if is_no_show else room_revenue

            # Create Climber reservation for this date
            climber_reservation = ClimberReservation(
                # Date fields
                record_date=record_date,
                calendar_date=calendar_date,
                calendar_date_start=calendar_date_start,
                calendar_date_end=calendar_date_end,
                created_date=created_date,
                # Guest and room info
                pax=reservation.pax,
                reservation_id=str(reservation.res_id),
                reservation_id_external=reservation_id_external,  # ResNo-GlobalResGuestId
                # Revenue fields
                revenue_fb=fb_revenue,
                revenue_fb_invoice=fb_revenue,
                revenue_others=other_revenue,
                revenue_others_invoice=other_revenue,
                revenue_room=room_revenue,
                revenue_room_invoice=room_revenue_invoice,
                # Reservation status
                rooms=rooms_count,  # 0 if date >= checkout
                status=status,
                # Segment codes
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

            climber_reservations.append(climber_reservation)

        return climber_reservations

    @staticmethod
    def _group_reservations_by_composite_key(
        host_reservations: list[HostReservation | dict[str, Any]],
    ) -> dict[tuple[int, int, int], list[HostReservation | dict[str, Any]]]:
        """Group reservations by (ResNo, ResId, GlobalResGuestId).

        This groups together multi-detail reservations that represent the same booking.

        Args:
            host_reservations: List of reservations from Host PMS API

        Returns:
            Dictionary mapping (ResNo, ResId, GlobalResGuestId) to list of reservations
        """
        groups: dict[tuple[int, int, int], list[HostReservation | dict[str, Any]]] = {}

        for reservation in host_reservations:
            if isinstance(reservation, dict):
                res_no = reservation.get("ResNo")
                res_id = reservation.get("ResId")
                global_res_guest_id = reservation.get("GlobalResGuestId")
            else:
                res_no = reservation.res_no
                res_id = reservation.res_id
                global_res_guest_id = reservation.global_res_guest_id

            key = (res_no, res_id, global_res_guest_id)
            if key not in groups:
                groups[key] = []
            groups[key].append(reservation)

        return groups

    @staticmethod
    def _get_price_dates_set(reservation: HostReservation | dict[str, Any]) -> set[str]:
        """Get set of unique price dates from a reservation.

        Args:
            reservation: Host PMS reservation

        Returns:
            Set of date strings (YYYY-MM-DD)
        """
        prices = reservation.get("Prices", []) if isinstance(reservation, dict) else reservation.prices
        date_set = set()
        for price_item in prices:
            if isinstance(price_item, dict):
                date_str = ReservationTransformer._get_date_string(price_item.get("Date"))
            else:
                date_str = ReservationTransformer._get_date_string(price_item.date)
            date_set.add(date_str)
        return date_set

    @staticmethod
    def transform_batch(
        host_reservations: list[HostReservation | dict[str, Any]],
        hotel_code: str = "UNKNOWN",
        reservation_status_map: dict[int, str] | None = None,
        hotel_local_time: datetime | str | None = None,
    ) -> tuple[ReservationCollection, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Transform multiple Host PMS reservations to Climber format using Prices array.

        Creates records based on Prices array dates, not CheckIn/CheckOut.

        For multi-detail reservations (same ResNo, ResId, GlobalResGuestId):
        - Groups them together
        - Detects overlapping price dates between master (MasterDetail=0) and children
        - For overlapping dates in child records: creates zero-revenue records with DetailId appended to reservation_external_id
        - For non-overlapping dates: processes normally

        reservation_id_external format:
        - Normal records: "{ResNo}-{GlobalResGuestId}" or "{ResNo}-{GlobalResGuestId}-{MasterDetail}"
        - Zero-revenue overlap records: "{ResNo}-{GlobalResGuestId}-{DetailId}"

        Skips duplicates with same (reservation_id_external, calendar_date), keeping first occurrence only.
        NOTE: Revenue consolidation is DISABLED - duplicates are skipped to avoid creating extra revenue.

        Args:
            host_reservations: List of reservations from Host PMS API
            hotel_code: Hotel code (metadata, not stored in each reservation)
            reservation_status_map: Optional dictionary mapping ConfigId integers to status code strings
            hotel_local_time: Optional hotel local time from HotelInfo for determining record_date start

        Returns:
            Tuple of (ReservationCollection, skipped_duplicates_list, composite_ids_list, consolidations_list)
            Note: consolidations_list will be empty since consolidation is disabled
        """
        if reservation_status_map is None:
            reservation_status_map = {}

        logger.info(
            "Batch transforming Host PMS reservations",
            hotel_code=hotel_code,
            reservation_count=len(host_reservations),
            status_map_count=len(reservation_status_map),
        )

        climber_reservations = []
        failed_count = 0
        consolidations_count = 0
        seen_reservation_nights = {}  # Track (reservation_id_external, calendar_date) -> index
        skipped_duplicates_list = []  # Not used in new algorithm
        composite_ids_list = []  # Not used in new algorithm
        consolidations_list = []  # Track revenue consolidations
        overlap_records_list = []  # Track zero-revenue overlap records

        # Step 1: Group reservations by (ResNo, ResId, GlobalResGuestId)
        reservation_groups = ReservationTransformer._group_reservations_by_composite_key(host_reservations)

        logger.info(
            "Grouped reservations by composite key",
            hotel_code=hotel_code,
            total_groups=len(reservation_groups),
        )

        # Step 2: Process each group
        for group_key, group_reservations in reservation_groups.items():
            res_no, res_id, global_res_guest_id = group_key

            # If only one reservation in group, process normally
            if len(group_reservations) == 1:
                host_reservation = group_reservations[0]
                try:
                    # Transform returns a list of reservations (one per price date)
                    climber_reservation_list = ReservationTransformer.transform(
                        host_reservation,
                        reservation_status_map,
                        hotel_local_time,
                        hotel_code,
                    )

                    # Add records (skip duplicates, do NOT consolidate revenues)
                    for climber_reservation in climber_reservation_list:
                        reservation_id_external = climber_reservation.reservation_id_external
                        calendar_date = climber_reservation.calendar_date
                        night_key = (reservation_id_external, calendar_date)

                        # Check if this (reservation_id_external, calendar_date) already exists
                        if night_key not in seen_reservation_nights:
                            # First occurrence - add it
                            seen_reservation_nights[night_key] = len(climber_reservations)
                            climber_reservations.append(climber_reservation)
                        else:
                            # Duplicate found - skip it (do NOT consolidate revenues)
                            logger.debug(
                                "Skipping duplicate reservation night (keeping first occurrence)",
                                hotel_code=hotel_code,
                                reservation_id_external=reservation_id_external,
                                calendar_date=calendar_date,
                            )
                except Exception as e:
                    logger.warning(
                        "Failed to transform single reservation in group",
                        hotel_code=hotel_code,
                        res_no=res_no,
                        error=str(e),
                    )
                    failed_count += 1
                continue

            # Multiple reservations in group - need to handle overlaps
            logger.info(
                "Processing multi-detail reservation group",
                hotel_code=hotel_code,
                res_no=res_no,
                res_id=res_id,
                global_res_guest_id=global_res_guest_id,
                count=len(group_reservations),
            )

            # Find master (MasterDetail == 0) and children (MasterDetail > 0)
            master_reservation = None
            child_reservations = []

            for reservation in group_reservations:
                if isinstance(reservation, dict):
                    master_detail = reservation.get("MasterDetail", 0)
                else:
                    master_detail = reservation.master_detail

                if master_detail == 0:
                    master_reservation = reservation
                else:
                    child_reservations.append(reservation)

            # Get master's price dates and Rooms value
            master_price_dates = set()
            master_rooms = 0
            if master_reservation is not None:
                master_price_dates = ReservationTransformer._get_price_dates_set(master_reservation)

                # Get Rooms value from master reservation
                if isinstance(master_reservation, dict):
                    master_rooms = master_reservation.get("Rooms", 0)
                else:
                    master_rooms = master_reservation.rooms

                # Process master normally
                try:
                    climber_reservation_list = ReservationTransformer.transform(
                        master_reservation,
                        reservation_status_map,
                        hotel_local_time,
                        hotel_code,
                    )

                    for climber_reservation in climber_reservation_list:
                        # For multi-detail master records, always use the Rooms value from data
                        # (don't apply checkout date zeroing logic)
                        climber_reservation.rooms = master_rooms

                        reservation_id_external = climber_reservation.reservation_id_external
                        calendar_date = climber_reservation.calendar_date
                        night_key = (reservation_id_external, calendar_date)

                        if night_key not in seen_reservation_nights:
                            seen_reservation_nights[night_key] = len(climber_reservations)
                            climber_reservations.append(climber_reservation)
                        else:
                            logger.debug(
                                "Skipping duplicate reservation night (keeping first occurrence)",
                                hotel_code=hotel_code,
                                reservation_id_external=reservation_id_external,
                                calendar_date=calendar_date,
                            )
                except Exception as e:
                    logger.warning(
                        "Failed to transform master reservation",
                        hotel_code=hotel_code,
                        res_no=res_no,
                        error=str(e),
                    )
                    failed_count += 1

            # Process children - check for overlaps
            for child_reservation in child_reservations:
                try:
                    if isinstance(child_reservation, dict):
                        detail_id = child_reservation.get("DetailId")
                    else:
                        detail_id = child_reservation.detail_id

                    child_price_dates = ReservationTransformer._get_price_dates_set(child_reservation)
                    overlapping_dates = child_price_dates & master_price_dates
                    non_overlapping_dates = child_price_dates - master_price_dates

                    if overlapping_dates:
                        logger.info(
                            "Found overlapping price dates in child reservation",
                            hotel_code=hotel_code,
                            res_no=res_no,
                            detail_id=detail_id,
                            overlapping_dates=sorted(list(overlapping_dates)),
                        )

                        # Create zero-revenue records for overlapping dates
                        climber_reservation_list = ReservationTransformer.transform(
                            child_reservation,
                            reservation_status_map,
                            hotel_local_time,
                            hotel_code,
                        )

                        for climber_reservation in climber_reservation_list:
                            calendar_date = climber_reservation.calendar_date

                            if calendar_date in overlapping_dates:
                                # Zero out revenue and rooms for this overlapping date
                                climber_reservation.revenue_room = 0.0
                                climber_reservation.revenue_room_invoice = 0.0
                                climber_reservation.revenue_fb = 0.0
                                climber_reservation.revenue_fb_invoice = 0.0
                                climber_reservation.revenue_others = 0.0
                                climber_reservation.revenue_others_invoice = 0.0
                                climber_reservation.rooms = 0

                                # Use format: ResNo-GlobalResGuestId-DetailId for overlap records
                                overlap_reservation_id = f"{res_no}-{global_res_guest_id}-{detail_id}"
                                climber_reservation.reservation_id_external = overlap_reservation_id

                                # Track this overlap record
                                overlap_records_list.append({
                                    "res_no": res_no,
                                    "global_res_guest_id": global_res_guest_id,
                                    "detail_id": detail_id,
                                    "calendar_date": calendar_date,
                                    "reservation_external_id": overlap_reservation_id,
                                })

                            # Add the record (whether zeroed or not)
                            reservation_id_external = climber_reservation.reservation_id_external
                            night_key = (reservation_id_external, calendar_date)

                            if night_key not in seen_reservation_nights:
                                seen_reservation_nights[night_key] = len(climber_reservations)
                                climber_reservations.append(climber_reservation)
                            else:
                                logger.debug(
                                    "Skipping duplicate reservation night (keeping first occurrence)",
                                    hotel_code=hotel_code,
                                    reservation_id_external=reservation_id_external,
                                    calendar_date=calendar_date,
                                )

                    elif non_overlapping_dates:
                        # Process non-overlapping dates normally
                        climber_reservation_list = ReservationTransformer.transform(
                            child_reservation,
                            reservation_status_map,
                            hotel_local_time,
                            hotel_code,
                        )

                        for climber_reservation in climber_reservation_list:
                            reservation_id_external = climber_reservation.reservation_id_external
                            calendar_date = climber_reservation.calendar_date
                            night_key = (reservation_id_external, calendar_date)

                            if night_key not in seen_reservation_nights:
                                seen_reservation_nights[night_key] = len(climber_reservations)
                                climber_reservations.append(climber_reservation)
                            else:
                                logger.debug(
                                    "Skipping duplicate reservation night (keeping first occurrence)",
                                    hotel_code=hotel_code,
                                    reservation_id_external=reservation_id_external,
                                    calendar_date=calendar_date,
                                )

                except Exception as e:
                    logger.warning(
                        "Failed to transform child reservation",
                        hotel_code=hotel_code,
                        res_no=res_no,
                        detail_id=detail_id,
                        error=str(e),
                    )
                    failed_count += 1

        # Create collection
        collection = ReservationCollection(
            reservations=climber_reservations,
        )

        logger.info(
            "Batch transformation complete",
            hotel_code=hotel_code,
            total_input_reservations=len(host_reservations),
            unique_nights_processed=len(seen_reservation_nights),
            reservation_groups=len(reservation_groups),
            overlap_records_created=len(overlap_records_list),
            successful_output=len(climber_reservations),
            failed_count=failed_count,
            total_count=len(climber_reservations),
        )

        # Return collection, skipped duplicates, composite IDs, and overlap records
        return collection, skipped_duplicates_list, composite_ids_list, overlap_records_list
