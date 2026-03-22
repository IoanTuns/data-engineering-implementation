"""Data management functions."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dataDP.config import COLUMNS_TO_NOT_UPDATE, TECHNICAL_AUDIT_COLUMNS
from dataDP.decorators import with_logging_and_spark
from dataDP.table_management import create_table_if_not_exists
from dataDP.utils.logger import logger


@with_logging_and_spark
def add_missing_columns_to_table(spark: SparkSession, table_name: str, source_schema: StructType) -> None:
    """
    Adds columns from source_schema to the target table if they don't exist.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        source_schema: PySpark StructType schema from the source DataFrame
    """

    # Get current target table schema
    try:
        target_schema = spark.table(table_name).schema
        target_columns = {field.name for field in target_schema.fields}
    except Exception:
        logger.info(f"Table {table_name} doesn't exist yet, will be created on first insert")
        return

    # Find columns in source but not in target
    missing_columns = []
    for field in source_schema.fields:
        if field.name not in target_columns:
            missing_columns.append(field)

    if not missing_columns:
        logger.info(f"No missing columns to add to {table_name}")
        return

    # Find columns in source but not in target
    missing_columns = []
    missing_audit_columns = []

    for field in source_schema.fields:
        if field.name not in target_columns:
            if field.name in TECHNICAL_AUDIT_COLUMNS:
                missing_audit_columns.append(field)
            else:
                missing_columns.append(field)

    # Combine: business columns first, then audit columns
    all_missing_columns = missing_columns + missing_audit_columns

    if not all_missing_columns:
        logger.info(f"No missing columns to add to {table_name}")
        return
    # Add business columns first
    if missing_columns:
        logger.info("  Business columns:")
        for field in missing_columns:
            col_name = field.name
            col_type = field.dataType.simpleString()

            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
            logger.info(f"Adding column {col_name} ({col_type}) to table {table_name}")

            try:
                spark.sql(alter_sql)
            except Exception as e:
                logger.warning(f"Could not add column {col_name}: {e}")

    # Add audit columns at the end
    if missing_audit_columns:
        logger.info("  Technical audit columns (added at end):")
        for field in missing_audit_columns:
            col_name = field.name
            col_type = field.dataType.simpleString()

            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
            logger.info(f"Adding audit column {col_name} ({col_type}) to table {table_name}")

            try:
                spark.sql(alter_sql)
            except Exception as e:
                logger.warning(f"Could not add audit column {col_name}: {e}")

    logger.info(f"Schema update complete for {table_name}")


@with_logging_and_spark
def insert_or_update_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    *,
    key_columns: list | None = None,
    force_remove_duplicates: bool = False,
    schema_evolution: bool = False,
    append_only: bool = False,
) -> None:
    """
    Inserts data from df into a Delta table.
    If force_remove_duplicates is True, removes duplicates from df based on key_columns before upsert.
    Parameters:
        spark (SparkSession): The Spark session.
        df (DataFrame): The DataFrame to be inserted into the table.
        table_name (str): The name of the Delta table.
        key_columns (list): List of columns to identify duplicates.
        force_remove_duplicates (bool): If True, removes duplicates from df before upsert.
        schema_evolution (bool): If True, allows schema evolution during upsert.
        append_only (bool): If True, only appends new rows to the table.
    Returns:
        None
    """
    deduped_df = df

    if force_remove_duplicates:
        if not key_columns:
            raise ValueError("key_columns must be provided when force_remove_duplicates is True")
        # Deduplicate df on key_columns before upsert
        deduped_df = df.dropDuplicates(key_columns)

    # Ensure table exists
    create_table_if_not_exists(table_name, df.schema)

    if append_only:
        # Append only mode
        (
            deduped_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true" if schema_evolution else "false")
            .saveAsTable(table_name)
        )
        return

    # Load the Delta table as an instance
    delta_table = DeltaTable.forName(spark, table_name)

    # Prepare merge condition
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
    # Identify columns to exclude from update
    existing_exclusions = [col for col in COLUMNS_TO_NOT_UPDATE if col in deduped_df.columns]
    logger.info(f"Excluding columns from update: {existing_exclusions}")
    # Identify columns to update
    columns_to_update = [col for col in deduped_df.columns if col not in existing_exclusions]
    logger.info(f"Updating columns: {columns_to_update}")

    # Build update dictionary
    update_dict = {col: f"source.{col}" for col in columns_to_update}

    # Perform merge
    merge_builder = delta_table.alias("target").merge(deduped_df.alias("source"), merge_condition)
    # Handle schema evolution if enabled
    if schema_evolution:
        add_missing_columns_to_table(table_name, deduped_df.schema)
        merge_builder = merge_builder.withSchemaEvolution()

    (merge_builder.whenMatchedUpdate(set=update_dict).whenNotMatchedInsertAll().execute())
