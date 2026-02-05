"""This module provides a function to insert a log record into the ingest_logs table after an ingestion process completes."""

from datetime import datetime

from pyspark.sql import SparkSession

from dataDP.data_management import insert_or_update_table
from dataDP.meta.tables_definitions import ingest_logs


def insert_ingestion_log(
    spark: SparkSession,
    *,
    execution_id: str,
    source_system: str,
    table_name: str,
    status: str,
    records_processed: int = 0,
    records_failed: int = 0,
    start_time: datetime,
    end_time: datetime,
    duration_min: str | int | float | None = None,
    message: str | None = None,
) -> None:
    """Insert a log record into the ingest_logs table after an ingestion process completes.
    Args:
        spark (SparkSession): The Spark session to use for database operations.
        execution_id (str): Unique identifier for the ingestion execution.
        source_system (str): Name of the source system being ingested.
        table_name (str): Name of the table being ingested.
        status (str): Status of the ingestion process (e.g., "success", "failure").
        records_processed (int, optional): Number of records successfully processed. Defaults to 0.
        records_failed (int, optional): Number of records that failed to process. Defaults to 0.
        start_time (datetime): Timestamp when the ingestion process started.
        end_time (datetime): Timestamp when the ingestion process ended.
        duration_min (str | int | float, optional): Duration of the ingestion process in minutes. Can be a string or numeric value. Defaults to None.
        message (str, optional): Additional message or details about the ingestion process. Defaults to None.
    """
    ingest_log_record = {
        "execution_id": execution_id,
        "source_system": source_system,
        "table_name": table_name,
        "status": status,
        "records_processed": records_processed,
        "records_failed": records_failed,
        "start_time": start_time,
        "end_time": end_time,
        "duration_min": duration_min,
        "message": message,
    }
    ingest_log_df = spark.createDataFrame([ingest_log_record], schema=ingest_logs.to_schema())
    insert_or_update_table(
        df=ingest_log_df,
        table_name=ingest_logs.table_name,
        append_only=True,
    )
