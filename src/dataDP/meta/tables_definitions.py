"""Tables definitions for metadata management."""

from dataDP.config import DEFAULT_INGEST_LOGS_TABLE, DEFAULT_SRC_DEFINITIONS_TABLE
from dataDP.table_management.definition import ColumnDefinition, TableDefinition, create_table_definition_from_dict

src_definitions_metadata = TableDefinition(
    table_name=DEFAULT_SRC_DEFINITIONS_TABLE,
    location="",
    columns=[
        ColumnDefinition("source_system", "string"),
        ColumnDefinition("volume", "string"),
        ColumnDefinition("stg_table", "string"),
        ColumnDefinition("select_columns", "string"),
        ColumnDefinition("filter_condition", "string"),
    ],
    key_columns=["source_system", "stg_table"],
)

ingest_logs = create_table_definition_from_dict(
    table_name=DEFAULT_INGEST_LOGS_TABLE,
    location="",
    columns_dict=[
        {"name": "execution_id", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "table_name", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "start_time", "type": "timestamp"},
        {"name": "end_time", "type": "timestamp"},
        {"name": "duration_min", "type": "integer"},
        {"name": "records_processed", "type": "integer"},
        {"name": "records_failed", "type": "integer"},
        {"name": "message", "type": "string", "comment": "Message logged during ingestion process"},
    ],
    include_is_active=False,
    include_create_and_update=False,
    include_valid_from_valid_to=False,
)
