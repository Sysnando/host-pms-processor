"""Climber padrão orchestration: single hotel, single timestamp, flow 1–8."""

from datetime import datetime, timedelta
from typing import Any

from structlog import get_logger

from src.aws import S3Manager, SQSManager
from src.aws.s3_manager import S3UploadError
from src.aws.sqs_manager import SQSError
from src.clients.esb_padrao_client import ESBPadraoClient, ESBPadraoError
from src.clients.host_api_client import HostPMSAPIClient
from src.config import settings
from src.transformers.config_transformer import ConfigTransformer
from src.transformers.stat_daily_to_reservation_transformer import (
    StatDailyToReservationTransformer,
)

logger = get_logger(__name__)


class ClimberPadraoOrchestrator:
    """Runs the Climber padrão flow: hotel code → download → raw S3 → transform → reservations/segments S3 → ESB → SQS.

    Uses HOTEL_CODE for PMS API and HOTEL_CODE_S3 for all S3/ESB/queue paths.
    One timestamp is used for all files (raw, reservations, segments).
    """

    def __init__(self):
        self.host_client = HostPMSAPIClient()
        self.s3_manager = S3Manager()
        self.sqs_manager = SQSManager()
        self.esb_padrao = ESBPadraoClient()

    @staticmethod
    def _timestamp_iso() -> str:
        """Single timestamp for this run (ISO up to seconds, UTC)."""
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    async def run(self) -> dict[str, Any]:
        """Execute flow 1–8. Returns summary dict; on critical failure logs and returns with success=False."""
        hotel_code = (settings.hotel_code or settings.hotel.hotel_code or "").strip()
        hotel_code_s3 = (settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or "").strip()

        missing = settings.validate_climber_padrao()
        if missing:
            logger.error("Climber padrão config missing", missing=missing)
            return {
                "success": False,
                "error": f"Missing config: {', '.join(missing)}",
                "timestamp": None,
                "reservations_count": 0,
                "segments_uploaded": False,
            }

        timestamp = self._timestamp_iso()
        logger.info(
            "Starting Climber padrão flow",
            hotel_code=hotel_code,
            hotel_code_s3=hotel_code_s3,
            timestamp=timestamp,
        )

        # 2. Download
        try:
            config_response = self.host_client.get_hotel_config(hotel_code)
        except Exception as e:
            logger.error("Download config failed", hotel_code=hotel_code, error=str(e))
            return {
                "success": False,
                "error": f"Download config: {e}",
                "timestamp": timestamp,
                "reservations_count": 0,
                "segments_uploaded": False,
            }

        today = datetime.utcnow().date()
        start_date = today - timedelta(days=settings.host_pms.stat_daily_days_back_start)
        end_date = today - timedelta(days=settings.host_pms.stat_daily_days_back_end)
        all_stat_daily: list[dict[str, Any]] = []
        current_date = start_date
        while current_date <= end_date:
            try:
                resp = self.host_client.get_stat_daily(
                    hotel_date_filter=current_date.isoformat(),
                    hotel_code=hotel_code,
                )
                if isinstance(resp, list):
                    all_stat_daily.extend(resp)
            except Exception as e:
                logger.warning(
                    "StatDaily fetch failed for date",
                    date=current_date.isoformat(),
                    error=str(e),
                )
            current_date += timedelta(days=1)

        if not all_stat_daily:
            logger.warning(
                "No StatDaily data; skipping uploads and ESB/SQS",
                hotel_code=hotel_code,
            )
            return {
                "success": True,
                "timestamp": timestamp,
                "reservations_count": 0,
                "segments_uploaded": False,
                "message": "No data to process",
            }

        # 3. Raw S3
        try:
            self.s3_manager.upload_raw_reservations(
                all_stat_daily,
                timestamp=timestamp,
                hotel_code_s3=hotel_code_s3,
            )
        except S3UploadError as e:
            logger.error("Upload raw failed", error=str(e))
            return {
                "success": False,
                "error": f"Upload raw: {e}",
                "timestamp": timestamp,
                "reservations_count": 0,
                "segments_uploaded": False,
            }

        # 4. Transform
        hotel_local_time = None
        try:
            from src.models.host.config import HotelConfigResponse

            cfg = (
                HotelConfigResponse(**config_response)
                if isinstance(config_response, dict)
                else config_response
            )
            if cfg and hasattr(cfg, "hotel_info") and cfg.hotel_info:
                hotel_local_time = getattr(cfg.hotel_info, "local_time", None)
        except Exception:
            pass

        reservation_collection = StatDailyToReservationTransformer.transform_batch(
            all_stat_daily,
            hotel_code=hotel_code,
            hotel_local_time=hotel_local_time,
            config_response=config_response,
        )
        _, segments_collection = ConfigTransformer.transform(config_response)

        if not reservation_collection.reservations and not segments_collection:
            logger.warning(
                "No reservations and no segments after transform",
                hotel_code=hotel_code,
            )
            return {
                "success": True,
                "timestamp": timestamp,
                "reservations_count": 0,
                "segments_uploaded": False,
                "message": "No reservations or segments",
            }

        # 5 & 6. Upload reservations and segments (same timestamp)
        try:
            self.s3_manager.upload_reservations(
                reservation_collection,
                timestamp=timestamp,
                hotel_code_s3=hotel_code_s3,
            )
        except S3UploadError as e:
            logger.error("Upload reservations failed", error=str(e))
            return {
                "success": False,
                "error": f"Upload reservations: {e}",
                "timestamp": timestamp,
                "reservations_count": len(reservation_collection.reservations),
                "segments_uploaded": False,
            }

        segments_uploaded = False
        try:
            self.s3_manager.upload_segments(
                segments_collection,
                timestamp=timestamp,
                hotel_code_s3=hotel_code_s3,
            )
            segments_uploaded = True
        except S3UploadError as e:
            logger.error("Upload segments failed", error=str(e))
            return {
                "success": False,
                "error": f"Upload segments: {e}",
                "timestamp": timestamp,
                "reservations_count": len(reservation_collection.reservations),
                "segments_uploaded": False,
            }

        # 7. ESB
        res_key = f"{hotel_code_s3}/reservations-{timestamp}.json"
        seg_key = f"{hotel_code_s3}/segments-{timestamp}.json"
        try:
            await self.esb_padrao.register_reservation_file(
                hotel_code_s3=hotel_code_s3,
                timestamp=timestamp,
                file_key=res_key,
            )
            await self.esb_padrao.register_segment_file(
                hotel_code_s3=hotel_code_s3,
                timestamp=timestamp,
                file_key=seg_key,
            )
        except ESBPadraoError as e:
            logger.error("ESB register failed", error=str(e))
            return {
                "success": False,
                "error": f"ESB: {e}",
                "timestamp": timestamp,
                "reservations_count": len(reservation_collection.reservations),
                "segments_uploaded": segments_uploaded,
            }

        # 8. SQS
        try:
            self.sqs_manager.send_processor_message(hotel_code_s3=hotel_code_s3)
        except SQSError as e:
            logger.error("SQS send failed", error=str(e))
            return {
                "success": False,
                "error": f"SQS: {e}",
                "timestamp": timestamp,
                "reservations_count": len(reservation_collection.reservations),
                "segments_uploaded": segments_uploaded,
            }

        logger.info(
            "Climber padrão flow complete",
            hotel_code_s3=hotel_code_s3,
            timestamp=timestamp,
            reservations_count=len(reservation_collection.reservations),
        )
        return {
            "success": True,
            "timestamp": timestamp,
            "reservations_count": len(reservation_collection.reservations),
            "segments_uploaded": segments_uploaded,
        }
