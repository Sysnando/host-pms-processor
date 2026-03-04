"""Main entry point for the Host PMS Connector application."""

import asyncio
import json
import sys
from typing import Any

from src.config import configure_logging, get_logger, settings
from src.services import HostPMSConnectorOrchestrator

logger = get_logger(__name__)


async def main() -> int:
    """Main async function to run the ETL pipeline.

    If HOTEL_CODE is set in environment, processes only that hotel.
    Otherwise, processes all hotels configured in Climber ESB.
    """
    logger.info(
        "Starting Host PMS Connector",
        environment=settings.environment,
    )

    try:
        orchestrator = HostPMSConnectorOrchestrator()

        # Check if specific hotel code is configured
        hotel_code = (settings.hotel_code or settings.hotel.hotel_code or "").strip()

        if hotel_code:
            # Single hotel mode
            logger.info("Processing single hotel from settings", hotel_code=hotel_code)
            result = await orchestrator.process_hotel(hotel_code)

            logger.info(
                "ETL pipeline complete",
                hotel_code=hotel_code,
                success=result["success"],
            )

            print(json.dumps(result, indent=2, default=str))
            return 0 if result["success"] else 1
        else:
            # Multi-hotel mode
            logger.info("Processing all hotels from ESB")
            results = await orchestrator.process_all_hotels()

            logger.info(
                "ETL pipeline complete",
                total_hotels=results["total_hotels"],
                successful_hotels=results["successful_hotels"],
                failed_hotels=results["failed_hotels"],
            )

            print(json.dumps(results, indent=2, default=str))

            if results["successful_hotels"] > 0:
                return 0
            elif results["failed_hotels"] == 0:
                logger.warning("No hotels were processed")
                return 1
            else:
                logger.error("All hotels failed to process")
                return 1

    except Exception as e:
        logger.error(
            "Fatal error in main application",
            error=str(e),
            exc_info=True,
        )
        return 1


def run_sync() -> int:
    """Run the async main function synchronously.

    Returns:
        Exit code from main()
    """
    return asyncio.run(main())


async def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda handler for the Host PMS Connector.

    Args:
        event: Lambda event (can contain override parameters)
        context: Lambda context

    Returns:
        Lambda response dictionary
    """
    logger.info(
        "Lambda invoked",
        environment=settings.environment,
        request_id=context.request_id,
    )

    try:
        # Create orchestrator
        orchestrator = HostPMSConnectorOrchestrator()

        # Check if specific hotel code is provided
        hotel_code = event.get("hotelCode") or event.get("hotel_code")

        if hotel_code:
            logger.info("Processing single hotel", hotel_code=hotel_code)
            result = await orchestrator.process_hotel(hotel_code)
            success = result["success"]
        else:
            logger.info("Processing all hotels")
            result = await orchestrator.process_all_hotels()
            success = result["successful_hotels"] > 0

        logger.info(
            "Lambda execution complete",
            request_id=context.request_id,
            success=success,
        )

        return {
            "statusCode": 200 if success else 400,
            "body": json.dumps(result, default=str),
        }

    except Exception as e:
        logger.error(
            "Lambda execution failed",
            request_id=context.request_id,
            error=str(e),
            exc_info=True,
        )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "request_id": context.request_id,
                }
            ),
        }


if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Run main function and exit with returned code
    sys.exit(run_sync())
