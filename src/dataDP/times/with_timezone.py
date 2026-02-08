"""Utility functions for handling timestamp columns with timezones in Spark DataFrames."""

from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampNTZType, TimestampType

from dataDP.utils.logger import logger


def get_timestamp_columns(df: DataFrame, include_ntz: bool = True, include_tz: bool = False) -> list:
    """
    Automatically detect timestamp columns in a DataFrame.

    Args:
        df: Source DataFrame
        include_ntz: Include TIMESTAMP_NTZ columns (default: True)
        include_tz: Include TIMESTAMP columns (default: False)

    Returns:
        List of column names that are timestamp types
    """
    timestamp_cols = []

    for field in df.schema.fields:
        if include_ntz and isinstance(field.dataType, TimestampNTZType):
            timestamp_cols.append(field.name)
        elif include_tz and isinstance(field.dataType, TimestampType):
            timestamp_cols.append(field.name)

    return timestamp_cols


def build_select_with_timezone_conversion(
    columns: str, timestamp_columns: list, source_timezone: str, convert_to_utc: bool = True
) -> str:
    """
    Build SELECT clause with automatic timezone conversion for timestamp columns.

    Args:
        columns: List of all column names or "*"
        timestamp_columns: List of timestamp column names to convert
        source_timezone: Source timezone (default: 'America/New_York')
        convert_to_utc: If True, convert to UTC; if False, just add timezone context

    Returns:
        SQL SELECT clause string
    """
    if columns == "*":
        # Need to handle this differently - can't mix * with individual columns
        select_parts = []
        return None  # Signal to handle differently

    select_parts = []
    columns_list = [c.strip() for c in columns.split(",")]

    for col in columns_list:
        if col in timestamp_columns:
            if convert_to_utc:
                # Convert to UTC timestamp
                select_parts.append(f"to_utc_timestamp({col}, '{source_timezone}') as {col}_utc")
            else:
                # Just cast to timezone-aware timestamp
                select_parts.append(f"CAST({col} AS TIMESTAMP) as {col}")
        else:
            select_parts.append(col)

    return ", ".join(select_parts)


def apply_timezone_conversion(
    df: DataFrame,
    *,
    source_timezone: str | None = None,
    select_columns: str | list | None = None,
    convert_to_utc: bool = True,
) -> DataFrame:
    """
    Apply timezone conversion to timestamp columns in a DataFrame.

    Args:
        df: Source DataFrame
        source_timezone: Source timezone (default: 'America/New_York')
        select_columns: A list or comma-separated string of column names to convert (default: None)
        convert_to_utc: If True, convert to UTC; if False, just add timezone context

    Returns:
        DataFrame with converted timestamp columns
    """
    if source_timezone is None:
        raise ValueError("source_timezone must be provided for timezone conversion")

    if select_columns:
        if isinstance(select_columns, str):
            select_columns = [c.strip() for c in select_columns.split(",")]
    else:
        select_columns = df.columns
    # Step 2: Detect timestamp columns
    timestamp_ntz_cols = get_timestamp_columns(df, include_ntz=True, include_tz=False)

    if timestamp_ntz_cols:
        logger.info(f"Detected TIMESTAMP_NTZ columns: {timestamp_ntz_cols}")
    else:
        logger.info("No TIMESTAMP_NTZ columns detected")

    # Step 3: Build SELECT with timezone conversion
    if timestamp_ntz_cols and select_columns != "*":
        # Parse select_columns and apply conversion
        columns_list = [c.strip() for c in select_columns]
        select_parts = []

        for col in columns_list:
            if col in timestamp_ntz_cols:
                if convert_to_utc:
                    # Convert to UTC
                    converted_name = f"{col}_utc"
                    select_parts.append(f"to_utc_timestamp({col}, '{source_timezone}') as {converted_name}")
                    logger.info(f"Converting {col} to UTC as {converted_name}")
                else:
                    # Keep original name but make timezone-aware
                    select_parts.append(f"to_utc_timestamp({col}, '{source_timezone}') as {col}")
                    logger.info(f"Converting {col} to timezone-aware TIMESTAMP")
            else:
                select_parts.append(col)

        select_clause = ", ".join(select_parts)
        return select_clause
    else:
        # No conversion needed or using *
        return select_columns
