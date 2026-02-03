"""Structured logging configuration using structlog."""
import logging
import sys
from typing import Any

import structlog
from pythonjsonlogger import jsonlogger

from src.config.settings import settings


def add_hotel_code_prefix(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add [HOTELCODE] prefix to log message if hotel_code is present.

    This processor runs before formatters to ensure the prefix appears
    in both JSON and console outputs.

    Args:
        logger: The logger instance
        method_name: The name of the method being called
        event_dict: The event dictionary containing log data

    Returns:
        Modified event dictionary with hotel code prefix
    """
    hotel_code = event_dict.get("hotel_code")
    if hotel_code:
        current_event = event_dict.get("event", "")
        event_dict["event"] = f"[{hotel_code}] {current_event}"
    return event_dict


def configure_logging() -> None:
    """Configure structlog for the application."""

    # Configure standard library logging
    log_level = getattr(logging, settings.logging.level)

    # Create handlers based on format
    handlers: list[logging.Handler] = []

    if settings.logging.format == "json":
        # JSON format for production
        json_handler = logging.StreamHandler(sys.stdout)
        json_formatter = jsonlogger.JsonFormatter()
        json_handler.setFormatter(json_formatter)
        json_handler.setLevel(log_level)
        handlers.append(json_handler)
    else:
        # Console format for development
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(log_level)
        handlers.append(console_handler)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    for handler in handlers:
        root_logger.addHandler(handler)

    # Silence noisy third-party loggers
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            add_hotel_code_prefix,  # Add [HOTELCODE] prefix before rendering
            structlog.processors.JSONRenderer()
            if settings.logging.format == "json"
            else structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Get a configured logger instance.

    Args:
        name: Logger name, typically __name__

    Returns:
        Configured logger instance
    """
    return structlog.get_logger(name)
