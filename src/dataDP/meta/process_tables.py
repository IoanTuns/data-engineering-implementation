"""Module for processing the process tables, including creating the table if it does not exist and inserting or updating records."""

from pyspark.sql import DataFrame, SparkSession

from dataDP.data_management.insert_or_update_table import insert_or_update_table
from dataDP.decorators import with_logging_and_spark
from dataDP.utils.technical_attributes import ensure_timestamp_columns


@with_logging_and_spark
def insert_or_update_process_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    key_columns: list = None,
    force_remove_duplicates: bool = False,
    schema_evolution: bool = True,
) -> None:
    """
    Inserts or updates data in the specified Delta table.
    If force_remove_duplicates is True, removes duplicates from spark_df based on key_columns before upsert.
    Parameters:
        spark (SparkSession): The Spark session.
        spark_df (DataFrame): The DataFrame to be inserted into the table.
        table_name (str): The name of the Delta table.
        key_columns (list): List of columns to identify duplicates.
        force_remove_duplicates (bool): If True, removes duplicates from spark_df before upsert.
        schema_evolution (bool): If True, enables schema evolution during upsert.
    Returns:
        None
    """

    # Ensure timestamp columns are present
    df, ts_cols = ensure_timestamp_columns(df)

    # Perform insert or update
    insert_or_update_table(df, table_name, key_columns=key_columns, schema_evolution=schema_evolution)
