"""Table and column definitions with ordered schema generation"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dataDP.decorators import with_logging


@with_logging
@dataclass
class ColumnDefinition:
    """
    Defines a single column with its name and datatype
    and provides a method to convert to PySpark DataType.

    How to use:
    ```
    table_def = TableDefinition(
    table_name="workspace.stg.yellow_taxi_tripdata",
    location="/Volumes/workspace/taxi/yellow",
    columns=[
        ColumnDefinition("vendor_id", "integer"),
        ColumnDefinition("pickup_datetime", "timestamp"),
        ColumnDefinition("dropoff_datetime", "timestamp"),
        ColumnDefinition("trip_distance", "double"),
        ColumnDefinition("store_and_fwd_flag", "string"),
        ColumnDefinition("fare_amount", "float"),
        ColumnDefinition("extra", "decimal(10,2)"),
        ]
    )

    spark_schema = table_def.to_schema()
    ```

    """

    name: str
    data_type: str  # e.g., 'string', 'integer', 'double', 'timestamp', 'decimal(10,2)'
    nullable: bool = True
    comment: str = None

    def to_spark_type(self):
        # Convert string data type to PySpark DataType
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "integer": IntegerType(),
            "long": LongType(),
            "bigint": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "bool": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
        }

        # Handle decimal types like 'decimal(10,2)'
        if self.data_type.startswith("decimal"):
            import re

            # Define regex to extract precision and scale
            match = re.match(r"decimal\((\d+),(\d+)\)", self.data_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 0)  # default

        return type_mapping.get(self.data_type.lower(), StringType())


def _add_is_active(fields: List[StructField]) -> List[StructField]:
    """Add is_active metadata column to a list of StructFields"""
    metadata_fields = [
        StructField("is_active", BooleanType(), False),
    ]
    return fields + metadata_fields


def _add_create_and_update(fields: List[StructField]) -> List[StructField]:
    """Add standard create and update metadata columns to a list of StructFields"""
    metadata_fields = [
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
    return fields + metadata_fields


def _add_valid_from_valid_to(fields: List[StructField]) -> List[StructField]:
    """Add standard create and update metadata columns to a list of StructFields"""
    metadata_fields = [
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
    ]
    return fields + metadata_fields


@with_logging
@dataclass
class TableDefinition:
    """Basic table definition preserving column order"""

    table_name: str
    location: str
    columns: List[ColumnDefinition]
    key_columns: List[str] = None
    include_is_active: bool = True
    include_create_and_update: bool = True
    include_valid_from_valid_to: bool = False

    def to_schema(self) -> StructType:
        """Create a PySpark StructType schema preserving column order"""
        fields = []

        # Parse columns string to list
        if self.columns:
            if isinstance(self.columns, list):
                # Already a list, use directly
                col_list = self.columns
            else:
                try:
                    # Try parsing as JSON array first
                    col_list = json.loads(self.columns)
                except (json.JSONDecodeError, TypeError):
                    # Fallback: treat as comma-separated string
                    col_list = [col.strip() for col in self.columns.split(",") if col.strip()]
            # Add data columns in order
            for col_item in col_list:
                # Check if it's a ColumnDefinition object or a string
                if hasattr(col_item, "name"):
                    # It's a ColumnDefinition object - use its properties
                    fields.append(StructField(col_item.name, col_item.to_spark_type(), col_item.nullable))
                else:
                    # It's a string - create a simple StringType field
                    fields.append(StructField(col_item, StringType(), True))

        # Add metadata columns if specified

        if self.include_valid_from_valid_to:
            fields = _add_valid_from_valid_to(fields)

        if self.include_is_active:
            fields = _add_is_active(fields)

        if self.include_create_and_update:
            fields = _add_create_and_update(fields)

        return StructType(fields)


@with_logging
def create_table_definition_from_dict(
    table_name: str,
    location: str,
    columns_dict: List[Dict[str, Any]],
    include_is_active: bool = True,
    include_create_and_update: bool = True,
    include_metadata: bool = True,
    include_valid_from_valid_to: bool = False,
) -> TableDefinition:
    """
    Helper to create TableDefinition from list of dicts

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        location: Storage location path
        columns_dict: List of column definitions as dicts with 'name', 'type', optional 'nullable', 'comment'
        include_metadata: Whether to include is_active, created_at, updated_at columns (default: True)

    Returns:
        TableDefinition instance
    """
    columns = [
        ColumnDefinition(
            name=col["name"], data_type=col["type"], nullable=col.get("nullable", True), comment=col.get("comment")
        )
        for col in columns_dict
    ]
    return TableDefinition(
        table_name, location, columns, include_is_active, include_create_and_update, include_valid_from_valid_to
    )
