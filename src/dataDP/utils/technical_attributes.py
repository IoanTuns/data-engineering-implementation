from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from dataDP.decorators import with_logging_and_spark


@with_logging_and_spark
def ensure_timestamp_columns(spark, df: DataFrame) -> (DataFrame, list):
    """Adds created_at and updated_at timestamp columns to the DataFrame."""
    # Define the timespan columns
    ts_cols = ["is_active", "created_at", "updated_at"]

    # Check if 'created_at' and 'updated_at' columns exist
    cols = df.columns

    # If both columns exist, return the DataFrame as is
    if all(col in cols for col in ts_cols):
        return (df, ts_cols)

    # Add missing columns
    if "is_active" not in cols:
        df = df.withColumn("is_active", lit(True))
    if "created_at" not in cols:
        df = df.withColumn("created_at", current_timestamp())
    if "updated_at" not in cols:
        df = df.withColumn("updated_at", current_timestamp())

    return (df, ts_cols)
