"""Main module for initializing environment and Spark context."""

import os

from dataDP.spark_context import get_spark_session
from dataDP.utils.logger import logger, setup_logger


def main() -> None:
    """Main entry point to initialize environment and Spark context."""

    # 1. Setup Logger first (Global configuration)
    log_destination = os.getenv("LOG_FILE_PATH")
    if log_destination:
        setup_logger(log_path=log_destination)
    else:
        setup_logger()
        logger.warning("LOG_FILE_PATH not set. Logs will only be displayed in console.")

    # 2. Initialize Spark
    # We use _spark to satisfy Ruff F841 (unused variable)
    _spark = get_spark_session("DataDP_Main_App")
    logger.info("Environment and Spark session are ready.")


if __name__ == "__main__":
    main()
