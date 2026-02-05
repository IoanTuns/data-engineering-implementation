from datetime import datetime
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as SPF

from dataDP.config import DEFAULT_SRC_DEFINITIONS_TABLE
from dataDP.data_management import insert_or_update_table
from dataDP.decorators import with_logging_and_spark
from dataDP.meta.insert_process_log import insert_ingestion_log
from dataDP.utils.get_run_id import get_execution_id
from dataDP.utils.logger import logger


@with_logging_and_spark
def populate_stg_tables(spark: SparkSession, tg_table_names: List[str], *, filter_condition: str | None = None):
    """
    Creates the staging tables defined in the metadata if they do not exist.
    This function should be run once during the initial setup of the data pipeline.
    """

    df = spark.sql(
        f"""select * from parquet.`{DEFAULT_SRC_DEFINITIONS_TABLE}` 
        where stg_table in {tg_table_names}
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
         FROM {volume}
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
