"""Provides a Spark session for local and Databricks environments."""

import os

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from dataDP.utils.logger import logger


def is_databricks_available() -> bool:
    """Checks if the script is running within a Databricks environment."""
    # DATABRICKS_RUNTIME_VERSION is only present on active clusters
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark_session(app_name="DataDP_Project", min_version="3.0") -> SparkSession:
    """
    Retrieves an existing Spark session or creates a new one, with version validation.

    Args:
        app_name (str): The name of the Spark application.
        min_version (str): The minimum Spark version required (e.g., "3.3").

    Returns:
        SparkSession: The active Spark session.
    """
    try:
        logger.info(f"Initializing Spark session: {app_name}")

        has_databricks = is_databricks_available()
        if has_databricks:
            spark = DatabricksSession.builder.appName(app_name).getOrCreate()
        else:
            spark = SparkSession.builder.appName(app_name).getOrCreate()

        # 1. Health check
        spark.sql("SELECT 1").collect()

        # 2. Version check
        current_version = spark.version
        if current_version < min_version:
            logger.warning(f"Spark version {current_version} is below the recommended {min_version}.")
        else:
            logger.info(f"Spark session established. Version: {current_version}")

        return spark

    except ImportError as e:
        logger.error("PySpark is not installed.")
        raise e
    except Exception as e:
        logger.error(f"Critical error during Spark initialization: {str(e)}")
        raise RuntimeError(f"Failed to start Spark context for {app_name}") from e
