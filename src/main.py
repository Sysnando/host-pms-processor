"""Main entry point for the Host PMS Connector application."""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Any

from src.config import configure_logging, get_logger, settings
from src.services import HostPMSConnectorOrchestrator

logger = get_logger(__name__)


async def main() -> int:
    """Main async function to run the ETL pipeline.

    If HOTEL_CODE_S3 is set in environment, processes only that hotel.
    Otherwise, processes all hotels configured in Climber ESB.

    Note: HOTEL_CODE and HOTEL_CODE_S3 both contain hotel codes.
    HOST_API_SUBSCRIPTION_KEY contains the subscription key for Host PMS API.
    """
    logger.info(
        "Starting Host PMS Connector",
        environment=settings.environment,
    )

    try:
        orchestrator = HostPMSConnectorOrchestrator()

        # Check if specific hotel code is configured (use hotel_code_s3 for Climber ESB)
        hotel_code_s3 = (settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or "").strip()

        if hotel_code_s3:
            # Single hotel mode
            logger.info("Processing single hotel from settings", hotel_code=hotel_code_s3)
            result = await orchestrator.process_hotel(hotel_code_s3)

            logger.info(
                "ETL pipeline complete",
                hotel_code=hotel_code_s3,
                success=result["success"],
            )

            print(json.dumps(result, indent=2, default=str))
            return 0 if result["success"] else 1
        else:
            # Multi-hotel mode
            # --hotel HOTELCODE filters to a single hotel while still using ESB credentials
            only_hotel = None
            if "--hotel" in sys.argv:
                idx = sys.argv.index("--hotel")
                if idx + 1 < len(sys.argv):
                    only_hotel = sys.argv[idx + 1].strip().upper()
                    logger.info("Filtering to single hotel", hotel_code=only_hotel)

            results = await orchestrator.process_all_hotels(only_hotel=only_hotel)

            logger.info(
                "ETL pipeline complete",
                total_hotels=results["total_hotels"],
                successful_hotels=results["successful_hotels"],
                failed_hotels=results["failed_hotels"],
            )

            print(json.dumps(results, indent=2, default=str))

            # Save full results to logs/
            os.makedirs("logs", exist_ok=True)
            ts = datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
            result_file = os.path.join("logs", f"etl_result_{ts}.json")
            with open(result_file, "w") as f:
                json.dump(results, f, indent=2, default=str)
            logger.info("ETL results saved", file=result_file)

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

        # Check if specific hotel code is provided (use hotel_code_s3 for Climber ESB)
        hotel_code_s3 = event.get("hotelCodeS3") or event.get("hotel_code_s3")

        if hotel_code_s3:
            logger.info("Processing single hotel", hotel_code=hotel_code_s3)
            result = await orchestrator.process_hotel(hotel_code_s3)
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
