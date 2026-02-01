"""DataDP entry point."""

from .spark_context import get_spark_session
from .utils.logger import logger

__all__ = ["get_spark_session", "logger"]


def main() -> None:
    """Main entry point to initialize environment and Spark context."""
    # Initialize Spark
    _spark = get_spark_session("DataDP_Main_App")
    logger.info("Environment and Spark session are ready.")


if __name__ == "__main__":
    main()
