"""Adaptive rate limiter for API requests.

This module provides an adaptive rate limiting mechanism that dynamically adjusts
concurrency based on API rate limit responses (429 status codes).
"""

import asyncio
from typing import Optional

from structlog import get_logger

from src.config import settings

logger = get_logger(__name__)


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts concurrency based on 429 responses.

    This class acts like an asyncio.Semaphore but with dynamic limits that adjust
    based on API rate limit feedback. When a 429 (rate limit) error is encountered,
    it reduces concurrency. After successful requests, it gradually recovers.

    Attributes:
        current_limit: Current concurrency limit
        min_limit: Minimum concurrency limit (won't go below this)
        max_limit: Maximum concurrency limit (initial value)
        success_count: Count of successful requests since last adjustment
        recovery_threshold: Number of successful requests before increasing limit
    """

    def __init__(
        self,
        initial_limit: Optional[int] = None,
        min_limit: Optional[int] = None,
        recovery_threshold: int = 10,
    ):
        """Initialize the adaptive rate limiter.

        Args:
            initial_limit: Initial concurrency limit (default: from settings)
            min_limit: Minimum concurrency limit (default: from settings)
            recovery_threshold: Number of successful requests before increasing limit
        """
        self.max_limit = initial_limit or settings.host_pms.rate_limit_initial_concurrency
        self.min_limit = min_limit or settings.host_pms.rate_limit_min_concurrency
        self.current_limit = self.max_limit
        self.recovery_threshold = recovery_threshold

        # Internal state
        self._semaphore = asyncio.Semaphore(self.current_limit)
        self._success_count = 0
        self._lock = asyncio.Lock()  # Protect state changes

        logger.info(
            "Adaptive rate limiter initialized",
            initial_limit=self.current_limit,
            min_limit=self.min_limit,
            max_limit=self.max_limit,
            recovery_threshold=recovery_threshold,
        )

    async def acquire(self):
        """Acquire a permit from the rate limiter.

        This method blocks if the current concurrency limit has been reached.
        """
        await self._semaphore.acquire()

    def release(self):
        """Release a permit back to the rate limiter."""
        self._semaphore.release()

    async def __aenter__(self):
        """Async context manager entry - acquire permit."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - release permit."""
        self.release()
        return False

    @staticmethod
    def _concurrency_for_rate_limit(rate_limit: int, time_window: Optional[str] = None) -> int:
        """Derive the appropriate concurrency from the API rate limit.

        Normalises the rate_limit to requests-per-second first, then maps:

            per_sec <= 20  →  1
            per_sec <= 30  →  2
            per_sec <= 60  →  3
            per_sec <= 100 →  5
            per_sec  > 100 →  per_sec // 20  (at least 5)
        """
        # Normalise to per-second
        if time_window and time_window.lower() == "minute":
            per_sec = rate_limit / 60
        elif time_window and time_window.lower() == "hour":
            per_sec = rate_limit / 3600
        else:
            # Default: assume per-second
            per_sec = rate_limit

        if per_sec <= 20:
            return 1
        if per_sec <= 30:
            return 2
        if per_sec <= 60:
            return 3
        if per_sec <= 100:
            return 5
        return max(5, int(per_sec) // 20)

    async def on_rate_limit(
        self,
        rate_limit: Optional[int] = None,
        time_window: Optional[str] = None,
        retry_after: Optional[float] = None,
    ):
        """Signal that a 429 rate limit error occurred.

        If rate_limit is provided (e.g. 10 for "10 per Second"), the concurrency
        is set to a value derived from that limit.  Otherwise falls back to
        halving the current concurrency.  If retry_after is provided, sleeps for
        that duration (outside the lock) before returning.

        Args:
            rate_limit: The rate limit value from API (e.g., 10 for "10 per Second")
            time_window: Time window for the rate limit (e.g., "Second", "Minute")
            retry_after: Seconds to wait as specified by the Retry-After response header
        """
        async with self._lock:
            old_limit = self.current_limit

            if rate_limit is not None:
                # Derive concurrency from the server-reported rate limit
                new_limit = max(self.min_limit, self._concurrency_for_rate_limit(rate_limit, time_window))
                # Also lower the recovery ceiling so on_success won't climb
                # back above what the API allows
                self.max_limit = min(self.max_limit, new_limit)
            else:
                # No rate limit info – fall back to halving
                new_limit = max(self.min_limit, self.current_limit // 2)

            if new_limit == old_limit:
                logger.warning(
                    "Rate limit hit but already at target concurrency",
                    current_limit=self.current_limit,
                    min_limit=self.min_limit,
                    api_rate_limit=rate_limit,
                )
            else:
                self.current_limit = new_limit
                self._success_count = 0
                self._semaphore = asyncio.Semaphore(self.current_limit)

                logger.warning(
                    "Rate limit exceeded - adjusting concurrency",
                    old_limit=old_limit,
                    new_limit=new_limit,
                    api_rate_limit=rate_limit,
                    retry_after=retry_after,
                )

        # Sleep outside the lock so other coroutines are not blocked
        if retry_after:
            logger.info("Waiting for Retry-After duration", retry_after=retry_after)
            await asyncio.sleep(retry_after)

    async def on_success(self):
        """Signal that a successful request occurred.

        After recovery_threshold successful requests, increases the concurrency
        limit by 1 (up to max_limit).
        """
        async with self._lock:
            self._success_count += 1

            # Check if we should increase limit
            if (
                self._success_count >= self.recovery_threshold
                and self.current_limit < self.max_limit
            ):
                old_limit = self.current_limit
                new_limit = min(self.max_limit, self.current_limit + 1)

                self.current_limit = new_limit
                self._success_count = 0  # Reset counter

                # Recreate semaphore with new limit
                self._semaphore = asyncio.Semaphore(self.current_limit)

                logger.info(
                    "Recovering concurrency limit",
                    old_limit=old_limit,
                    new_limit=new_limit,
                    max_limit=self.max_limit,
                    success_count_threshold=self.recovery_threshold,
                )

    def get_current_limit(self) -> int:
        """Get the current concurrency limit.

        Returns:
            Current concurrency limit
        """
        return self.current_limit


# Global singleton instance
# This is shared across all API calls to coordinate rate limiting
adaptive_rate_limiter = AdaptiveRateLimiter()
