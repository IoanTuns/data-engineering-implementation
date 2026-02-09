from datetime import datetime
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as SPF

from dataDP.config import DEFAULT_SRC_DEFINITIONS_TABLE
from dataDP.data_management import insert_or_update_table
from dataDP.decorators import with_logging_and_spark
from dataDP.meta.insert_process_log import insert_ingestion_log
from dataDP.times.with_timezone import apply_timezone_conversion
from dataDP.utils.get_run_id import get_execution_id
from dataDP.utils.logger import logger


@with_logging_and_spark
def populate_stg_tables_from_storage(
    spark: SparkSession,
    tg_table_names: List[str],
    *,
    file_format: str = "parquet",
    filter_condition: str | None = None,
    timezone_to_utc: bool = False,
    source_timezone: str | None = None,
    csv_header: bool = False,
    csv_delimiter: str = ",",
):
    """
    Creates the staging tables defined in the metadata if they do not exist.
    This function should be run once during the initial setup of the data pipeline.
    Args:
        spark: SparkSession object
        tg_table_names: List of staging table names to create and populate
        file_format: File format of the source data (default: 'parquet')
        filter_condition: Optional filter condition to apply when selecting data from source
        timezone_to_utc: If True, applies timezone conversion to UTC for timestamp columns
        source_timezone: Source timezone for timestamp conversion (required if timezone_to_utc is True)
    Returns:
        None
    """

    # Input validations
    if timezone_to_utc and source_timezone is None:
        logger.error("source_timezone must be provided when timezone_to_utc is True")
        raise ValueError("source_timezone must be provided when timezone_to_utc is True")

    if file_format.lower() not in ["parquet", "delta", "csv", "json", "xml", "avro", "orc", "text", "binary"]:
        logger.error(f"Unsupported file format: {file_format}. Supported formats are 'parquet' and 'delta'.")
        raise ValueError(f"Unsupported file format: {file_format}. Supported formats are 'parquet' and 'delta'.")

    # Use start time from global variable if available, otherwise set it to current time
    if "start_time" in globals():
        start_time = globals()["start_time"]
    else:
        start_time = datetime.now()

    # Check if list of table names
    if isinstance(tg_table_names, list) or isinstance(tg_table_names, tuple):
        if len(tg_table_names) > 1:
            df = spark.sql(
                f"""select * from {DEFAULT_SRC_DEFINITIONS_TABLE}
                where stg_table in {tg_table_names}
                and is_active = true
                """
            )
        else:
            tg_table_names = tg_table_names[0]

    if isinstance(tg_table_names, str):
        df = spark.sql(
            f"""select * from {DEFAULT_SRC_DEFINITIONS_TABLE}
            where stg_table = '{tg_table_names}'
            and is_active = true
            """
        )

    if df.isEmpty():
        logger.warning("No staging tables to process.")
        return

    table_defs = df.collect()
    logger.info(f"Found {len(table_defs)} staging tables to process.")
    for row in table_defs:
        logger.info(
            f"Staging table definition: stg_table={row['stg_table']}, volume={row['volume']}, select_columns={row['select_columns']}, filter_condition={row['filter_condition']}"
        )
    for row in table_defs:
        stg_table_name = row["stg_table"]
        logger.info(f"Processing staging table: {stg_table_name}")
        volume = row["volume"]
        select_columns = row["select_columns"]
        filter_condition = row["filter_condition"]

        # Create staging table if it does not exist
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {stg_table_name}")
            logger.info(f"Staging table {stg_table_name} created or already exists")
        except Exception as e:
            logger.warning(f"Error creating staging table {stg_table_name}: {e}")

        # source_timezone can be collected from meta table. Validation logic has to be changes

        # Apply timezone conversion to UTC for timestamp columns if enabled
        if timezone_to_utc:
            logger.info(f"Applying timezone conversion to UTC for staging table {stg_table_name}")

            logger.info(f"Detecting schema from {volume}")
            sample_df = spark.read.format(file_format).load(volume).limit(1)

            select_columns = apply_timezone_conversion(sample_df, source_timezone=source_timezone)

        else:
            spark.sql(f"""ALTER TABLE {stg_table_name}
                            SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')""")

        # Where condition handling - if filter_condition is provided in metadata, use it; otherwise, no filtering

        where_condition = df.filter(filter_condition).collect()[0][0] if filter_condition else None

        where_clauses = []
        if filter_condition:
            for filter in filter_condition.split(";"):
                where_clauses.append(filter.strip())

        if where_condition:
            for condition in where_condition.split(";"):
                where_clauses.append(condition.strip())

        if where_clauses:
            where_clause = " AND ".join(where_clauses)

        # Create staging table definition based on metadata
        sql = f"""
         SELECT 
            {select_columns},
            current_timestamp() as loaded_at,
            _metadata.file_name as source_file_name,
            _metadata.file_path as source_file_path,
            _metadata.file_modification_time as source_file_modified_at
         FROM '{volume}'
        {f"WHERE {where_clause}" if where_clauses else ""}
        """

        copy_sql = f"""
            COPY INTO {stg_table_name}
            FROM (
                {sql}
            )
            FILEFORMAT = {file_format.upper()}
            """

        format_options = ["'mergeSchema' = 'true'"]
        if file_format.lower() == "csv":
            format_options.append(f"'header' = '{csv_header}'")
            format_options.append(f"'delimiter' = '{csv_delimiter}'")

        copy_sql += f"FORMAT_OPTIONS ({', '.join(format_options)})\n"
        copy_sql += "COPY_OPTIONS ('mergeSchema' = 'true')"

        print(copy_sql)

        try:
            logger.info(f"Copying data to staging table {stg_table_name} from volume {volume}")
            logger.debug(f"Copying data to staging table {stg_table_name} with SQL: {copy_sql}")
            result = spark.sql(copy_sql)
            affected_rows = result.collect()[0]["num_affected_rows"]
            logger.info(f"Successfully loaded {affected_rows} rows into {stg_table_name}")

            file_stats_df = spark.sql(f"""
                SELECT 
                    source_file_name,
                    COUNT(*) as row_count,
                    MAX(source_file_modified_at) as file_modified_at
                FROM {stg_table_name}
                WHERE loaded_at >= timestamp'{start_time.isoformat()}'
                GROUP BY source_file_name
                ORDER BY source_file_name
            """)

            insert_ingestion_log(
                spark=spark,
                execution_id=get_execution_id(),
                source_system=row["volume"],
                table_name=stg_table_name,
                status="success",
                records_processed=affected_rows,
                start_time=start_time,
                end_time=datetime.now(),
                duration_min=(datetime.now() - start_time).total_seconds() / 60,
                message=f"Staging table {stg_table_name} load from a total of {file_stats_df.count()} files.",
            )

            for file_row in file_stats_df.collect():
                insert_ingestion_log(
                    spark=spark,
                    execution_id=get_execution_id(),
                    source_system=row["volume"],
                    table_name=stg_table_name,
                    status="success",
                    records_processed=file_row["row_count"],
                    message=f"{file_row['source_file_name']}.",
                )
        except Exception as e:
            logger.error(f"Error copying data to staging table {stg_table_name}: {e}")
            insert_ingestion_log(
                spark=spark,
                execution_id=get_execution_id(),
                source_system=row["volume"],
                table_name=stg_table_name,
                status="failed",
                records_processed=0,
                start_time=start_time,
                end_time=datetime.now(),
                duration_min=(datetime.now() - start_time).total_seconds() / 60,
                message=f"Error: {str(e)}",
            )
            raise


@with_logging_and_spark
def populate_stg_tables_from_sql(
    spark: SparkSession, tg_table_names: List[str], *, filter_condition: str | None = None
):
    """
    Creates the staging tables defined in the metadata if they do not exist.
    This function should be run once during the initial setup of the data pipeline.
    """

    df = spark.sql(
        f"""select * from {DEFAULT_SRC_DEFINITIONS_TABLE}
        where stg_table in {tuple(tg_table_names) if len(tg_table_names) > 1 else f"('{tg_table_names[0]}')"}
        and is_active = true
        """
    )

    if df.isEmpty():
        logger.warning("No staging tables to process.")
        return

    table_defs = df.collect()
    for row in table_defs:
        stg_table_name = row["stg_table"]
        logger.info(f"Processing staging table: {stg_table_name}")
        volume = row["volume"]
        select_columns = row["select_columns"]
        filter_condition = row["filter_condition"]

        # Create staging table definition based on metadata
        sql = f"""
         SELECT {select_columns}
         FROM parquet.`{volume}`
        """
        if filter_condition:
            sql += f" WHERE {filter_condition}"

        records_count = spark.sql(sql).count()

        if "start_time" in globals():
            start_time = globals()["start_time"]
        else:
            start_time = datetime.now()

        df = spark.sql(sql)

        df_inget_files = df.withColumn("file_name", SPF.input_file_name()).groupBy("file_name").count()

        insert_or_update_table(
            df=df,
            table_name=stg_table_name,
            append_only=True,
        )

        insert_ingestion_log(
            spark=spark,
            execution_id=get_execution_id(),
            source_system=row["volume"],
            table_name=stg_table_name,
            status="success",
            records_processed=records_count,
            start_time=start_time if "start_time" in locals() else datetime.now(),
            end_time=datetime.now(),
            duration_min=(datetime.now() - start_time).total_seconds() / 60,
            message=f"Staging table {stg_table_name} load from a total of {df_inget_files.count()} files.",
        )

        for file_row in df_inget_files.collect():
            insert_ingestion_log(
                spark=spark,
                execution_id=get_execution_id(),
                source_system=row["volume"],
                table_name=stg_table_name,
                status="success",
                records_processed=file_row["count"],
                message=f"{file_row['file_name']}.",
            )
