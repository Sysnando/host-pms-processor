"""Redis-based OAuth token manager for ESB authentication."""

from typing import Any, Optional

import httpx
import redis.asyncio as redis
from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class TokenManagerError(Exception):
    """Raised when token management fails."""

    pass


class RedisTokenManager:
    """Manages OAuth tokens with Redis caching for shared use across processes."""

    REDIS_KEY = "esb:oauth:token"
    TOKEN_TTL = 3000  # 50 minutes in seconds (less than 1 hour to be safe)

    def __init__(self):
        """Initialize Redis client for token caching."""
        self.redis_client = redis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            password=settings.redis.password,
            ssl=settings.redis.ssl,
            decode_responses=True,
            socket_timeout=settings.redis.socket_timeout,
            socket_connect_timeout=settings.redis.socket_connect_timeout,
        )

    async def get_auth_token(self) -> str:
        """Get OAuth token from Redis cache or fetch new one.

        This method implements the get-or-refresh pattern recommended by Vinicius:
        1. Try to get token from Redis
        2. If null, fetch new token via OAuth
        3. Store in Redis with TTL

        Returns:
            Valid OAuth access token

        Raises:
            TokenManagerError: If token fetch fails
        """
        # Step 1: Try to get from Redis
        try:
            cached_token = await self._get_cached_token()
            if cached_token:
                logger.debug("Using cached OAuth token from Redis")
                return cached_token
        except Exception as e:
            logger.warning(
                "Failed to get token from Redis, will fetch new token",
                error=str(e),
            )

        # Step 2: If null, fetch new token via OAuth
        logger.info("Fetching new OAuth token from ESB")
        try:
            token_data = await self._fetch_new_token()
            token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)

            # Step 3: Store in Redis with TTL (use 50min instead of full 1h for safety buffer)
            # Clamp TTL to at least 1 second to avoid non-positive values when expires_in is short-lived
            ttl = max(1, min(expires_in - 600, self.TOKEN_TTL))  # 10 min buffer
            await self._store_token(token, ttl)

            return token

        except Exception as e:
            logger.error(
                "Failed to fetch OAuth token",
                error=str(e),
                exc_info=True,
            )
            raise TokenManagerError(f"Failed to obtain OAuth token: {str(e)}") from e

    async def _get_cached_token(self) -> Optional[str]:
        """Get token from Redis cache.

        Returns:
            Cached token if available, None otherwise
        """
        try:
            token = await self.redis_client.get(self.REDIS_KEY)
            return token if token else None
        except Exception as e:
            logger.warning("Redis get operation failed", error=str(e))
            return None

    async def _store_token(self, token: str, ttl: int) -> None:
        """Store token in Redis with TTL.

        Args:
            token: OAuth access token
            ttl: Time to live in seconds
        """
        try:
            await self.redis_client.setex(self.REDIS_KEY, ttl, token)
            logger.info(
                "Stored OAuth token in Redis",
                ttl_seconds=ttl,
            )
        except Exception as e:
            logger.warning(
                "Failed to store token in Redis (will still use token)",
                error=str(e),
            )
            # Don't raise - token is still valid even if caching fails

    async def _fetch_new_token(self) -> dict[str, Any]:
        """Fetch new OAuth token from ESB.

        Returns:
            OAuth token response with access_token and expires_in

        Raises:
            httpx.HTTPError: If OAuth request fails
        """
        token_url = f"{settings.esb.base_url.rstrip('/')}{settings.esb.oauth_token_url}"

        payload = {
            "grant_type": settings.esb.oauth_grant_type,
            "client_id": settings.esb.oauth_client_id,
            "client_secret": settings.esb.oauth_client_secret,
        }

        logger.debug(
            "Requesting OAuth token",
            token_url=token_url,
            grant_type=settings.esb.oauth_grant_type,
        )

        async with httpx.AsyncClient(timeout=settings.esb.request_timeout) as client:
            response = await client.post(
                token_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if response.status_code != 200:
                logger.error(
                    "OAuth token request failed",
                    status_code=response.status_code,
                    response_text=response.text,
                )
                response.raise_for_status()

            token_data = response.json()
            logger.info(
                "Successfully fetched OAuth token",
                expires_in=token_data.get("expires_in"),
            )
            return token_data

    async def close(self) -> None:
        """Close Redis connection.

        Should be called when the token manager is no longer needed.
        """
        try:
            await self.redis_client.close()
            logger.debug("Closed Redis connection")
        except Exception as e:
            logger.warning("Error closing Redis connection", error=str(e))
