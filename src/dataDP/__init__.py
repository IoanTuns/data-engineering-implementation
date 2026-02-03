"""DataDP entry point."""

from dataDP.spark_context import get_spark_session
from dataDP.utils.logger import logger

__all__ = ["get_spark_session", "logger"]
