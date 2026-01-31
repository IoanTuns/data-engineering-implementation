"""Provides a Spark session for local and Databricks environments."""
from pyspark.sql import SparkSession

def get_spark_session():
    """
    Return the active Spark session or create a new one.
    Utility for running code locally and on Databricks.
    """
    return SparkSession.builder.getOrCreate()