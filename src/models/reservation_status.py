"""Reservation status mapping between Host PMS and Climber formats."""

from enum import IntEnum


class ReservationStatus(IntEnum):
    """Climber reservation status enum.

    Maps to Climber V2 reservation status codes:
    - 0: CANCELLED
    - 1: CHECKED_IN
    - 2: CHECKED_OUT
    - 3: CONFIRMED
    - 4: NO_SHOW
    - 5: TENTATIVE
    """
    CANCELLED = 0
    CHECKED_IN = 1
    CHECKED_OUT = 2
    CONFIRMED = 3
    NO_SHOW = 4
    TENTATIVE = 5


class ReservationStatusMapper:
    """Maps Host PMS reservation status codes to Climber format."""

    @staticmethod
    def map_host_status_to_climber(host_status_code: str) -> int:
        """Map Host PMS reservation status code to Climber status enum.

        Mapping logic based on Host PMS config:
        - "CI" (ConfigId 10) → CHECKED_IN (1)
        - "CO" (ConfigId 20) → CHECKED_OUT (2)
        - "NOSHOW" (ConfigId 7) → NO_SHOW (4)
        - "OPTION" (ConfigId 2) → TENTATIVE (5)
        - "CXL" (ConfigId 6) → CANCELLED (0)
        - "OOI" (ConfigId 8) → CANCELLED (0)
        - "OOO" (ConfigId 5) → CANCELLED (0)
        - "WAITLIST" (ConfigId 3) → CANCELLED (0)
        - "STANDARD" (ConfigId 0) / default → CONFIRMED (3)

        Args:
            host_status_code: Status code from Host PMS (e.g., "CI", "CO", "OPTION")

        Returns:
            Climber reservation status code (0-5)
        """
        status_mapping = {
            "CI": ReservationStatus.CHECKED_IN,
            "CO": ReservationStatus.CHECKED_OUT,
            "NOSHOW": ReservationStatus.NO_SHOW,
            "OPTION": ReservationStatus.TENTATIVE,
            "CXL": ReservationStatus.CANCELLED,
            "OOI": ReservationStatus.CANCELLED,
            "OOO": ReservationStatus.CANCELLED,
            "WAITLIST": ReservationStatus.CANCELLED,
            "STANDARD": ReservationStatus.CONFIRMED,
        }

        # Default to CONFIRMED if status code not found
        return status_mapping.get(host_status_code, ReservationStatus.CONFIRMED)

    @staticmethod
    def build_status_code_map(reservation_statuses: dict[str, str]) -> dict[str, int]:
        """Build a mapping from Host PMS status codes to Climber status enums.

        Args:
            reservation_statuses: Dictionary mapping Host PMS status codes to descriptions
                                  (e.g., {"CI": "Checked-in", "CO": "Checked-out"})

        Returns:
            Dictionary mapping Host PMS status codes to Climber status integers
        """
        status_map = {}
        for status_code in reservation_statuses.keys():
            climber_status = ReservationStatusMapper.map_host_status_to_climber(status_code)
            status_map[status_code] = climber_status

        return status_map
