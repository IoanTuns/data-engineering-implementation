"""Module for creating a Delta table if it does not exist, with error handling for schema mismatches."""

from pyspark.sql.types import StructType

from dataDP.decorators import with_logging_and_spark
from dataDP.exceptions.tables import TableCreationErrorException
from dataDP.utils.logger import logger


@with_logging_and_spark
def create_table_if_not_exists(spark, table_name: str, struct_schema: StructType) -> bool:
    """
    Inserts or updates data in the specified Delta table.
    If force_remove_duplicates is True, removes duplicates from spark_df based on key_columns before upsert.
    Parameters:
        spark (SparkSession): The Spark session.
        struct_schema (StructType): The schema of the table to be created.
        table_name (str): The name of the Delta table.
    Returns:
        bool: True if table was created, False if it already existed.
    """
    if spark.catalog.tableExists(table_name):
        logger.info(f"Table {table_name} exists. Performing insert or update.")
        return False

    else:
        logger.info(f"Table {table_name} does not exist. Creating it.")

        empty_df = spark.createDataFrame([], struct_schema)

        try:
            (
                empty_df.write.format("delta")
                .mode("ignore")  # Silently ignores if table exists
                .saveAsTable(table_name)
            )

            logger.info(f"Table '{table_name}' created successfully.")
            return True

        except Exception as e:
            msg = f"Failed to create table '{table_name}'"
            logger.error(msg, exc_info=True)
            raise TableCreationErrorException(message=msg, expected_schema=str(struct_schema), actual_schema=str(e))
