"""Local test script for running the pipeline without AWS/Climber infrastructure.

This script uses the LocalTestOrchestrator to test the full pipeline locally:
- Fetches real data from Host PMS API
- Saves files to data_extracts directory instead of S3
- Logs ESB registrations instead of calling API (or uses real ESB if USE_REAL_ESB=true)
- Logs SQS messages instead of sending

Usage:
    # Mock ESB (default - no real API calls)
    python -m tests.test_local_run

    # Real ESB with Redis + OAuth authentication
    USE_REAL_ESB=true python -m tests.test_local_run

    # Combined with hotel code and custom output
    USE_REAL_ESB=true HOTEL_CODE_S3=QUATRO_VIAS_SA OUTPUT_DIR=./my_output python -m tests.test_local_run

Environment Variables:
    - USE_REAL_ESB: Set to 'true' to use real ESB client (default: false)
    - HOTEL_CODE_S3: Climber hotel code to process (if not set, processes all hotels)
    - OUTPUT_DIR: Directory to save files (default: ./data_extracts)
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
    # Get configuration from environment
    output_dir = os.getenv("OUTPUT_DIR", "./data_extracts")
    use_real_esb = os.getenv("USE_REAL_ESB", "false").lower() == "true"

    logger.info(
        "Starting Local Test Pipeline",
        environment=settings.environment,
        output_dir=output_dir,
        use_real_esb=use_real_esb,
    )

    try:
        # Create orchestrator with custom settings
        orchestrator = LocalTestOrchestrator(
            output_dir=output_dir,
            use_real_esb=use_real_esb,
        )

        # Check if specific hotel code is configured
        hotel_code = (settings.hotel_code_s3 or settings.hotel.hotel_code_s3 or "").strip()

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

    # Check if real ESB is enabled
    use_real_esb = os.getenv("USE_REAL_ESB", "false").lower() == "true"

    # Print banner
    print("\n" + "=" * 80)
    print("HOST PMS CONNECTOR - LOCAL TEST MODE")
    print("=" * 80)
    print("This test runs the full pipeline WITHOUT uploading to S3 or sending SQS")
    print("Files will be saved locally for inspection")
    print("=" * 80)

    if use_real_esb:
        print("ESB Mode: REAL (Redis + OAuth authentication)")
        print(f"ESB URL: {os.getenv('ESB_BASE_URL', 'from settings')}")
        print(f"Redis: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}")
    else:
        print("ESB Mode: MOCK (no real API calls)")

    print("=" * 80 + "\n")

    # Run main function and exit with returned code
    sys.exit(run_sync())
