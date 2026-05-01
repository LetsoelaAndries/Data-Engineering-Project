"""
Main orchestrator for the Nedbank DE Challenge pipeline.
Runs bronze, silver, and gold layers in sequence.
"""

import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run the complete pipeline end-to-end."""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Nedbank DE Challenge Pipeline Starting")
    logger.info(f"Start time: {start_time}")
    logger.info("=" * 60)

    try:
        # Stage 1 & 2: Bronze, Silver, Gold layers
        logger.info("-" * 40)
        logger.info("STAGE 1 & 2: Batch Pipeline")
        logger.info("-" * 40)

        logger.info("Starting Bronze layer (ingest)...")
        from pipeline.ingest import main as ingest_main
        ingest_main()
        logger.info("Bronze layer completed successfully")

        logger.info("Starting Silver layer (transform)...")
        from pipeline.transform import main as transform_main
        transform_main()
        logger.info("Silver layer completed successfully")

        logger.info("Starting Gold layer (provision)...")
        from pipeline.provision import main as provision_main
        provision_main()
        logger.info("Gold layer completed successfully")

        # Stage 3: Streaming layer (only if running in Stage 3 environment)
        # The streaming directory may not exist in Stage 1/2, so we try-catch
        logger.info("-" * 40)
        logger.info("STAGE 3: Streaming Pipeline (checking...)")
        logger.info("-" * 40)

        import os
        from pipeline.config_loader import get_config

        config = get_config()
        streaming_config = config.get('streaming')

        if streaming_config and os.path.exists(streaming_config.get('stream_input_path', '')):
            logger.info("Streaming input directory detected. Starting stream ingestion...")
            from pipeline.stream_ingest import main as stream_main
            stream_main()
            logger.info("Streaming layer completed successfully")
        else:
            logger.info("No streaming input directory found. Skipping Stage 3.")

        # Calculate and log total execution time
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Pipeline completed successfully")
        logger.info(f"End time: {end_time}")
        logger.info(f"Total duration: {duration:.2f} seconds")
        logger.info("=" * 60)

        sys.exit(0)

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"PIPELINE FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()