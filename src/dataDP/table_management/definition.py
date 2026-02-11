"""Table and column definitions with ordered schema generation"""

import operator
import re
from dataclasses import dataclass
from functools import reduce
from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as PSF
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
    Defines a single column with its name, data type, and validation rules.

    This class encapsulates the definition of a table column, including its
    schema properties (name, type, nullability) and data quality rules
    (min/max values, custom SQL conditions). It provides methods to convert
    this definition into PySpark schema components and validation expressions.

    Attributes:
        name (str): The name of the column.
        data_type (str): The data type as a string (e.g., 'string', 'integer', 'decimal(10,2)').
        nullable (bool): Whether the column allows null values. Defaults to True.
        comment (str): Optional description or comment for the column.
        min_value (Optional[Any]): Minimum allowed value for validation.
        max_value (Optional[Any]): Maximum allowed value for validation.
        custom_condition (Optional[str]): A custom SQL expression for validation (e.g., "length(col) > 5").

    Example:
        ```python
        col_def = ColumnDefinition(
            name="fare_amount",
            data_type="double",
            min_value=0.0,
            comment="The fare amount in dollars"
        )
        spark_field = col_def.to_spark_field()
        ```
    """

    name: str
    data_type: str  # e.g., 'string', 'integer', 'double', 'timestamp', 'decimal(10,2)'
    nullable: bool = True
    comment: str = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    custom_condition: Optional[str] = None

    def to_spark_field(self) -> StructField:
        """
        Converts the column definition to a PySpark StructField.

        Returns:
            StructField: A PySpark StructField object with the specified name,
                data type, nullability, and metadata (comment).
        """
        return StructField(
            self.name,
            self.to_spark_type(),
            nullable=self.nullable,
            metadata={"comment": self.comment} if self.comment else None,
        )

    def to_spark_type(self):
        """
        Converts the string representation of the data type to a PySpark DataType.

        Supports common types like 'string', 'integer', 'timestamp', etc., as well
        as 'decimal(p,s)'. Defaults to StringType if the type is unknown.

        Returns:
            DataType: The corresponding PySpark DataType instance.
        """
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
            # Define regex to extract precision and scale
            match = re.match(r"decimal\((\d+),(\d+)\)", self.data_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(10, 0)  # default

        return type_mapping.get(self.data_type.lower(), StringType())

    def get_validation_expr(self) -> Optional[PSF.Column]:
        """
        Generates a PySpark Column expression for data validation based on defined rules.

        Combines checks for nullability, minimum value, maximum value, and custom
        conditions into a single boolean expression.

        Returns:
            Optional[PSF.Column]: A boolean Column expression representing the validation logic,
                or None if no validation rules are defined.
        """
        conditions = []
        col = PSF.col(self.name)

        if not self.nullable:
            conditions.append(col.isNotNull())
        if self.min_value is not None:
            conditions.append(col >= self.min_value)
        if self.max_value is not None:
            conditions.append(col <= self.max_value)
        if self.custom_condition:
            conditions.append(PSF.expr(self.custom_condition))

        if not conditions:
            return None

        # Combine all conditions with AND
        final_expr = reduce(operator.and_, conditions)
        return final_expr


def _add_is_active(fields: List[StructField]) -> List[StructField]:
    """Appends an 'is_active' metadata column to a list of StructFields.

    This helper function is used to add a standard metadata column to a schema
    definition. The 'is_active' column is defined as a non-nullable boolean,
    typically used for soft-deletes.

    Args:
        fields (List[StructField]): The existing list of PySpark StructField objects.

    Returns:
        List[StructField]: The list of StructFields with the 'is_active' field appended.
    """
    metadata_fields = [
        StructField("is_active", BooleanType(), False),
    ]
    return fields + metadata_fields


def _add_create_and_update(fields: List[StructField]) -> List[StructField]:
    """Appends standard 'created_at' and 'updated_at' metadata columns.

    This helper function adds standard timestamp columns for tracking record
    creation and modification times. Both columns are defined as non-nullable
    Timestamps.

    Args:
        fields (List[StructField]): The existing list of PySpark StructField objects.

    Returns:
        List[StructField]: The list of StructFields with 'created_at' and
            'updated_at' fields appended.
    """
    metadata_fields = [
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
    return fields + metadata_fields


def _add_valid_from_valid_to(fields: List[StructField]) -> List[StructField]:
    """Appends 'valid_from' and 'valid_to' metadata columns for SCD Type 2.

    This helper function adds standard timestamp columns used to track the
    validity period of a record, a common pattern in Slowly Changing
    Dimensions (SCD) Type 2 implementations. 'valid_from' is non-nullable,
    while 'valid_to' is nullable to represent the currently active record.

    Args:
        fields (List[StructField]): The existing list of PySpark StructField objects.

    Returns:
        List[StructField]: The list of StructFields with 'valid_from' and
            'valid_to' fields appended.
    """
    metadata_fields = [
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
    ]
    return fields + metadata_fields


@with_logging
@dataclass
class TableDefinition:
    """
    Represents the complete definition of a Delta table, including schema, metadata, and validation rules.

    This class acts as a central blueprint for a table. It defines the table's
    name, location, column structure, primary keys, and which standard metadata
    columns (like 'is_active', 'created_at') should be included. It provides
    methods to generate a PySpark schema, DDL statements, and perform data
    validation against the defined rules.

    Attributes:
        table_name (str): The fully qualified name of the table (e.g., 'catalog.schema.table').
        location (str): The cloud storage path where the table data is stored.
        columns (List[ColumnDefinition]): An ordered list of ColumnDefinition objects.
        key_columns (Optional[List[str]]): A list of column names that form the natural key of the table.
        version (str): The schema version, stored as a table property. Defaults to "1.0.0".
        include_is_active (bool): If True, adds an 'is_active' boolean column for soft deletes. Defaults to True.
        include_create_and_update (bool): If True, adds 'created_at' and 'updated_at' timestamp columns. Defaults to True.
        include_valid_from_valid_to (bool): If True, adds 'valid_from' and 'valid_to' columns for SCD Type 2. Defaults to False.

    Example:
        ```python
        from dataDP.table_management import ColumnDefinition, TableDefinition

        table_def = TableDefinition(
            table_name="workspace.stg.yellow_taxi_tripdata",
            location="/mnt/data/yellow_taxi",
            columns=[
                ColumnDefinition("vendor_id", "integer", nullable=False),
                ColumnDefinition("trip_distance", "double", min_value=0.0),
            ],
            key_columns=["vendor_id", "pickup_datetime"],
            include_valid_from_valid_to=True
        )

        spark_schema = table_def.to_schema()
        ddl_statement = table_def.generate_ddl()
        ```
    """

    table_name: str
    location: str
    columns: List[ColumnDefinition]
    key_columns: Optional[List[str]] = None
    version: str = "1.0.0"
    include_is_active: bool = True
    include_create_and_update: bool = True
    include_valid_from_valid_to: bool = False

    def __post_init__(self):
        """Validates the table definition after initialization.

        Ensures that the table name is provided, the columns list is not empty,
        and that conflicting metadata options are not selected.
        """
        if not self.table_name:
            raise ValueError("Field 'table_name' cannot be empty.")
        if not self.columns:
            raise ValueError("List 'columns' must contain at least one column definition.")
        if self.include_create_and_update and self.include_valid_from_valid_to:
            raise ValueError(
                "Cannot include both 'create_and_update' and 'valid_from_valid_to' metadata columns simultaneously."
            )

    def to_schema(self) -> StructType:
        """
        Generates a PySpark StructType schema from the table definition.

        The schema is constructed based on the `columns` list, preserving the specified
        order. It also appends standard metadata columns (`is_active`, `created_at`, etc.)
        if their respective `include_*` flags are set to True.

        Returns:
            StructType: The complete PySpark schema for the table.
        """
        fields = [col.to_spark_field() for col in self.columns]

        # Add metadata columns if specified
        if self.include_valid_from_valid_to:
            fields = _add_valid_from_valid_to(fields)
        if self.include_is_active:
            fields = _add_is_active(fields)
        if self.include_create_and_update:
            fields = _add_create_and_update(fields)

        return StructType(fields)

    def compare_schema(self, actual_df: DataFrame) -> Dict[str, List[str]]:
        """Compares the DataFrame's schema against the table definition's schema.

        This method is useful for schema validation, identifying discrepancies between
        an incoming DataFrame and the expected table structure.

        Args:
            actual_df (DataFrame): The PySpark DataFrame whose schema will be compared.

        Returns:
            Dict[str, List[str]]: A dictionary detailing the discrepancies, with keys:
                - 'missing_in_source': Columns expected by the definition but not in the DataFrame.
                - 'unexpected_in_source': Columns in the DataFrame but not in the definition.
                - 'type_mismatches': Columns with a different data type than expected.
        """
        actual_fields = {f.name: f.dataType for f in actual_df.schema.fields}
        expected_fields = {f.name: f.dataType for f in self.to_schema().fields}

        report = {"missing_in_source": [], "unexpected_in_source": [], "type_mismatches": []}

        # 1. Check for missing or type-mismatched columns
        for name, expected_type in expected_fields.items():
            if name not in actual_fields:
                report["missing_in_source"].append(name)
            elif actual_fields[name] != expected_type:
                report["type_mismatches"].append(f"{name}: expected {expected_type}, got {actual_fields[name]}")

        # 2. Check for extra columns in source
        for name in actual_fields:
            if name not in expected_fields:
                report["unexpected_in_source"].append(name)

        return report

    def generate_ddl(self, table_format: str = "delta") -> str:
        """Generates a 'CREATE TABLE IF NOT EXISTS' DDL statement for the table.

        The generated SQL statement includes column definitions with data types,
        nullability constraints, and comments. It also specifies the table format,
        location, and sets a 'schema_version' table property.

        Args:
            table_format (str): The table format provider (e.g., 'delta', 'parquet').
                Defaults to 'delta'.

        Returns:
            str: A formatted SQL DDL string.
        """
        column_specs = []
        for col in self.columns:
            # Construct the line: name type [NOT NULL] [COMMENT '...']
            spec = f"  `{col.name}` {col.data_type.upper()}"

            if not col.nullable:
                spec += " NOT NULL"

            if col.comment:
                spec += f" COMMENT '{col.comment}'"

            column_specs.append(spec)

        column_section = ",\n".join(column_specs)

        ddl = f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
            {column_section}
            )
            USING {table_format}
            LOCATION '{self.location}'
            TBLPROPERTIES ('schema_version' = '{self.version}');"""

        return ddl

    def generate_constraints(self) -> List[str]:
        """Generates Delta Lake CHECK constraints for hard enforcement."""
        constraints = []
        for col in self.columns:
            if col.min_value is not None:
                constraint_name = f"{col.name}_min_check"
                sql = f"ALTER TABLE {self.table_name} ADD CONSTRAINT {constraint_name} CHECK ({col.name} >= {col.min_value});"
                constraints.append(sql)
            if col.max_value is not None:
                constraint_name = f"{col.name}_max_check"
                sql = f"ALTER TABLE {self.table_name} ADD CONSTRAINT {constraint_name} CHECK ({col.name} <= {col.max_value});"
                constraints.append(sql)
            # TODO: Implement custom_condition check
        return constraints

    def validate_dataframe(self, to_write_df: DataFrame) -> DataFrame:
        """Validates a DataFrame against the rules defined in the columns.

        This method applies all validation rules (is_not_null, min/max value, custom
        conditions) from the `ColumnDefinition` list and adds a new boolean column,
        'is_row_valid', to the DataFrame. A row is marked `True` if it passes all
        checks, and `False` otherwise.

        Args:
            to_write_df (DataFrame): Data to be inserted in form of a DataFrame to be validated.

        Returns:
            DataFrame: The original DataFrame with an added 'is_row_valid' boolean column.
        """
        validation_cols = []
        for col_def in self.columns:
            expr = col_def.get_validation_expr()
            if expr:
                validation_cols.append(expr)

        # If all conditions are met, row is valid
        if validation_cols:
            combined_cond = reduce(operator.and_, validation_cols)
            return to_write_df.withColumn("is_row_valid", combined_cond)
        return to_write_df.withColumn("is_row_valid", PSF.lit(True))

    def validate_with_reasons(self, to_write_df: DataFrame) -> DataFrame:
        """Validates a DataFrame and provides detailed reasons for any failures.

        For each row, this method evaluates all column validation rules. It adds two
        new columns to the DataFrame:
        - 'validation_errors': An array of strings, where each string is a message
          describing a failed validation rule for that row. The array is empty if
          the row is valid.
        - 'is_row_valid': A boolean flag that is `True` if 'validation_errors' is empty,
          and `False` otherwise.

        Args:
            to_write_df (DataFrame): Data to be inserted in form of a DataFrame to be validated.

        Returns:
            DataFrame: The original DataFrame with 'validation_errors' and 'is_row_valid' columns appended.
        """
        failure_messages = []
        for col_def in self.columns:
            expr = col_def.get_validation_expr()
            if expr:
                # If the expression is FALSE, return the error message, else NULL
                msg = f"Violation in {col_def.name}: condition failed"
                failure_messages.append(PSF.when(~expr, PSF.lit(msg)))

        # Combine all messages into a single array column
        return to_write_df.withColumn(
            "validation_errors", PSF.array_remove(PSF.array(*failure_messages), None)
        ).withColumn("is_row_valid", PSF.size(PSF.col("validation_errors")) == 0)

    def process_data(
        self, to_write_df: DataFrame, *, one_output_table: bool = False
    ) -> Union[Tuple[DataFrame, DataFrame], DataFrame]:
        """Validates a DataFrame and can either split it or return a single annotated DataFrame.

        This method acts as a core data quality processing engine. It uses
        `validate_with_reasons` to assess each row and then partitions the input
        DataFrame based on the validation outcome.

        Args:
            to_write_df (DataFrame): Data to be inserted in the form of a DataFrame to be validated.
            one_output_table (bool): If False (default), splits the DataFrame into two
                (clean and quarantined). If True, returns a single DataFrame with
                'is_row_valid' and 'validation_errors' columns.

        Returns:
            Union[Tuple[DataFrame, DataFrame], DataFrame]:
                - If `one_output_table` is False, a tuple containing two DataFrames:
                  (clean_df, quarantine_df). The clean_df contains only valid rows,
                  and quarantine_df contains invalid rows with validation details.
                - If `one_output_table` is True, a single DataFrame with validation
                  columns ('is_row_valid', 'validation_errors') added.
        """
        validated_df = self.validate_with_reasons(to_write_df)
        if one_output_table:
            return validated_df

        # Split logic
        clean_df = validated_df.filter("is_row_valid == true").drop("is_row_valid", "validation_errors")

        quarantine_df = validated_df.filter("is_row_valid == false")

        return clean_df, quarantine_df

    def sync_table_version(self, spark: SparkSession):
        """Updates the schema_version TBLPROPERTY of the table."""
        self.logger.info(f"Syncing table version for {self.table_name} to {self.version}")
        spark.sql(f"ALTER TABLE {self.table_name} SET TBLPROPERTIES ('schema_version' = '{self.version}')")

    def ensure_version_sync(self, spark: SparkSession):
        """
        Ensures the table's schema_version property in the catalog matches the definition.

        If the table exists, it compares the 'schema_version' table property. If they
        don't match, it updates the property. If the table does not exist, it creates
        it using the DDL from `generate_ddl`.
        """
        from pyspark.sql.utils import AnalysisException

        try:
            # 1. Fetch existing properties from the Spark Catalog
            props = spark.sql(f"SHOW TBLPROPERTIES {self.table_name}").collect()
            # Convert the list of rows into a dictionary
            prop_dict = {row["key"]: row["value"] for row in props}
            current_catalog_version = prop_dict.get("schema_version")

            # 2. Compare and Update if necessary
            if current_catalog_version != self.version:
                self.logger.warning(
                    f"Version mismatch for {self.table_name}! "
                    f"Catalog version: {current_catalog_version}, Code version: {self.version}. Updating..."
                )
                self.sync_table_version(spark)
            else:
                self.logger.info(f"Table {self.table_name} is already at version {self.version}.")

        except AnalysisException as e:
            # If the table doesn't exist, AnalysisException is thrown.
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                self.logger.info(f"Table {self.table_name} not found. Running initial DDL to create it.")
                spark.sql(self.generate_ddl())
            else:
                # Re-raise other analysis exceptions
                raise e


@with_logging
def create_table_definition_from_dict(
    table_name: str,
    location: str,
    columns_dict: List[Dict[str, Any]],
    key_columns: Optional[List[str]] = None,
    include_is_active: bool = True,
    include_create_and_update: bool = True,
    include_valid_from_valid_to: bool = False,
) -> TableDefinition:
    """Creates a TableDefinition instance from a dictionary-based configuration.

    This helper function simplifies the creation of a TableDefinition object,
    allowing for a more declarative, configuration-driven approach.

    Args:
        table_name (str): Fully qualified table name (e.g., 'catalog.schema.table').
        location (str): The cloud storage path for the table data.
        columns_dict (List[Dict[str, Any]]): A list of dictionaries, where each
            dictionary defines a column. Required keys are 'name' and 'type'.
            Optional keys include 'nullable', 'comment', 'min_value', 'max_value',
            and 'custom_condition'.
        key_columns (Optional[List[str]]): List of column names that form the natural key. Defaults to None.
        include_is_active (bool): If True, adds an 'is_active' column. Defaults to True.
        include_create_and_update (bool): If True, adds 'created_at' and 'updated_at' columns. Defaults to True.
        include_valid_from_valid_to (bool): If True, adds 'valid_from' and 'valid_to' columns. Defaults to False.

    Returns:
        TableDefinition: An instantiated TableDefinition object.
    """
    columns = [
        ColumnDefinition(
            name=col["name"],
            data_type=col["type"],
            nullable=col.get("nullable", True),
            comment=col.get("comment"),
            min_value=col.get("min_value"),
            max_value=col.get("max_value"),
            custom_condition=col.get("custom_condition"),
        )
        for col in columns_dict
    ]
    return TableDefinition(
        table_name=table_name,
        location=location,
        columns=columns,
        key_columns=key_columns,
        include_is_active=include_is_active,
        include_create_and_update=include_create_and_update,
        include_valid_from_valid_to=include_valid_from_valid_to,
    )
