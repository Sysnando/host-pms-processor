"""Local test script for running the pipeline without AWS/Climber infrastructure.

This script uses the LocalTestOrchestrator to test the full pipeline locally:
- Fetches real data from Host PMS API
- Saves files to data_extract directory instead of S3
- Logs ESB registrations instead of calling API
- Logs SQS messages instead of sending

Usage:
    # Process single hotel (saves to ./data_extract)
    HOTEL_CODE=HOTEL001 python -m tests.test_local_run

    # Process all hotels (from mocked ESB list)
    python -m tests.test_local_run

    # Specify custom output directory
    OUTPUT_DIR=./my_output HOTEL_CODE=HOTEL001 python -m tests.test_local_run
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import configure_logging, get_logger, settings
from tests.local_test_orchestrator import LocalTestOrchestrator

logger = get_logger(__name__)


async def main() -> int:
    """Run the local test pipeline.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Get output directory from environment or use default
    output_dir = os.getenv("OUTPUT_DIR", "./data_extracts")

    logger.info(
        "Starting Local Test Pipeline",
        environment=settings.environment,
        output_dir=output_dir,
    )

    try:
        # Create orchestrator with custom output directory
        orchestrator = LocalTestOrchestrator(output_dir=output_dir)

        # Check if specific hotel code is configured
        hotel_code = (settings.hotel_code or settings.hotel.hotel_code or "").strip()

        if hotel_code:
            # Single hotel mode
            logger.info(
                "Processing single hotel (local test)",
                hotel_code=hotel_code,
                output_dir=output_dir,
            )
            result = await orchestrator.process_hotel(hotel_code)

            logger.info(
                "Local test pipeline complete",
                hotel_code=hotel_code,
                success=result["success"],
            )

            # Print results
            print("\n" + "=" * 80)
            print("LOCAL TEST RESULTS (Single Hotel)")
            print("=" * 80)
            print(json.dumps(result, indent=2, default=str))
            print("=" * 80)
            print(f"\nFiles saved to: {Path(output_dir).absolute()}")
            print("=" * 80 + "\n")

            return 0 if result["success"] else 1

        else:
            # Multi-hotel mode
            logger.info(
                "Processing all hotels (local test)",
                output_dir=output_dir,
            )
            results = await orchestrator.process_all_hotels()

            logger.info(
                "Local test pipeline complete",
                total_hotels=results["total_hotels"],
                successful_hotels=results["successful_hotels"],
                failed_hotels=results["failed_hotels"],
            )

            # Print results
            print("\n" + "=" * 80)
            print("LOCAL TEST RESULTS (All Hotels)")
            print("=" * 80)
            print(json.dumps(results, indent=2, default=str))
            print("=" * 80)
            print(f"\nFiles saved to: {Path(output_dir).absolute()}")
            print("=" * 80 + "\n")

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
            "Fatal error in local test pipeline",
            error=str(e),
            exc_info=True,
        )
        print(f"\nERROR: {str(e)}\n")
        return 1


def run_sync() -> int:
    """Run the async main function synchronously.

    Returns:
        Exit code from main()
    """
    return asyncio.run(main())


if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Print banner
    print("\n" + "=" * 80)
    print("HOST PMS CONNECTOR - LOCAL TEST MODE")
    print("=" * 80)
    print("This test runs the full pipeline WITHOUT uploading to S3/ESB/SQS")
    print("Files will be saved locally for inspection")
    print("=" * 80 + "\n")

    # Run main function and exit with returned code
    sys.exit(run_sync())
