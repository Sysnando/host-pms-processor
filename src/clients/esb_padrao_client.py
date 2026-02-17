"""Climber padrão ESB client: OAuth via Basic Auth + register reservation/segment files."""

from typing import Any, Optional

import httpx
from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class ESBPadraoError(Exception):
    """ESB padrão operation failed."""

    pass


class ESBPadraoClient:
    """ESB client for Climber padrão: token from ESB_AUTH_URL + ESB_BASIC_AUTH, register files."""

    def __init__(self):
        # Prefer top-level (from .env ESB_*); fallback to nested esb.*
        self.auth_url = (settings.esb_auth_url or settings.esb.auth_url or "").strip() or ""
        self.reservations_url = (settings.esb_reservations_url or settings.esb.reservations_url or "").strip() or ""
        self.segments_url = (settings.esb_segments_url or settings.esb.segments_url or "").strip() or ""
        self.basic_auth = (settings.esb_basic_auth or settings.esb.basic_auth or "").strip() or ""
        self.timeout = settings.esb.request_timeout

    def _timestamp_iso_seconds(self, timestamp: str) -> str:
        """Ensure timestamp is ISO up to seconds (e.g. 2024-07-04T11:26:32Z)."""
        if not timestamp:
            return timestamp
        ts = timestamp.strip()
        if "T" in ts:
            base = ts[:19] if len(ts) >= 19 else ts
            return base + "Z" if not base.endswith("Z") else base
        return ts

    async def _get_token(self) -> str:
        """Get OAuth token using ESB_BASIC_AUTH at ESB_AUTH_URL."""
        if not self.basic_auth:
            raise ESBPadraoError("ESB_BASIC_AUTH is not set")
        headers = {
            "Authorization": f"Basic {self.basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.auth_url,
                headers=headers,
                data=data,
            )
            if response.status_code != 200:
                logger.error(
                    "ESB token request failed",
                    status_code=response.status_code,
                    response_text=response.text[:500],
                )
                raise ESBPadraoError(
                    f"Token request failed: {response.status_code} {response.text}"
                )
            body = response.json()
            token = body.get("access_token")
            if not token:
                raise ESBPadraoError("No access_token in ESB response")
            return token

    async def _post_payload(
        self,
        url: str,
        payload_body: dict[str, Any],
        token: str,
    ) -> dict[str, Any]:
        """POST JSON payload with Bearer token."""
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url,
                headers=headers,
                json=payload_body,
            )
            # 200 OK, 201 Created, 202 Accepted, 204 No Content = success
            if response.status_code not in (200, 201, 202, 204):
                logger.error(
                    "ESB register request failed",
                    url=url,
                    status_code=response.status_code,
                    response_text=response.text[:500],
                )
                raise ESBPadraoError(
                    f"Register failed: {response.status_code} {response.text}"
                )
            return response.json() if response.text else {}

    async def register_reservation_file(
        self,
        hotel_code_s3: str,
        timestamp: str,
        file_key: Optional[str] = None,
    ) -> dict[str, Any]:
        """Register reservation file in ESB (Climber padrão payload).

        Payload: { "payload": { "code": "<HOTEL_CODE_S3>", "record_date": "<ts>",
        "last_updated": "<ts>", "complete": false, "file": "<key>" } }
        file_key = S3 key without bucket (e.g. BRNPEDCO/reservations-2024-07-04T11:26:32Z.json).
        """
        ts = self._timestamp_iso_seconds(timestamp)
        key = file_key or f"{hotel_code_s3}/reservations-{ts}.json"
        payload = {
            "payload": {
                "code": hotel_code_s3,
                "record_date": ts,
                "last_updated": ts,
                "complete": False,
                "file": key,
            }
        }
        token = await self._get_token()
        logger.info(
            "Registering reservation file in ESB (padrão)",
            hotel_code_s3=hotel_code_s3,
            file=key,
        )
        result = await self._post_payload(
            self.reservations_url,
            payload,
            token,
        )
        logger.info(
            "Registered reservation file",
            hotel_code_s3=hotel_code_s3,
            file=key,
        )
        return result

    async def register_segment_file(
        self,
        hotel_code_s3: str,
        timestamp: str,
        file_key: Optional[str] = None,
    ) -> dict[str, Any]:
        """Register segment file in ESB (Climber padrão payload).

        file_key = S3 key without bucket (e.g. BRNPEDCO/segments-2024-07-04T11:26:32Z.json).
        """
        ts = self._timestamp_iso_seconds(timestamp)
        key = file_key or f"{hotel_code_s3}/segments-{ts}.json"
        payload = {
            "payload": {
                "code": hotel_code_s3,
                "record_date": ts,
                "last_updated": ts,
                "complete": False,
                "file": key,
            }
        }
        token = await self._get_token()
        logger.info(
            "Registering segment file in ESB (padrão)",
            hotel_code_s3=hotel_code_s3,
            file=key,
        )
        result = await self._post_payload(
            self.segments_url,
            payload,
            token,
        )
        logger.info(
            "Registered segment file",
            hotel_code_s3=hotel_code_s3,
            file=key,
        )
        return result
