"""Custom exceptions for data processing operations."""


class DataFormatError(Exception):
    """Exception raised for unsupported or invalid data formats during ingestion.

    Attributes:
        message -- explanation of the error
        data_format -- the data format that caused the error
    """

    def __init__(self, message: str, data_format: str | None = None):
        self.message = message
        self.data_format = data_format
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Data Format: {self.data_format}]"


class DataValidationError(Exception):
    """Exception raised for data validation failures.

    Attributes:
        message -- explanation of the error
        record -- the data record that failed validation
    """

    def __init__(self, message: str, record: dict | None = None):
        self.message = message
        self.record = record
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Record: {self.record}]"


class DataProcessingError(Exception):
    """Exception raised for errors during data processing operations.

    Attributes:
        message -- explanation of the error
        step -- the processing step where the error occurred
    """

    def __init__(self, message: str, step: str | None = None):
        self.message = message
        self.step = step
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Processing Step: {self.step}]"


class DataNotFoundError(Exception):
    """Exception raised when expected data is not found.

    Attributes:
        message -- explanation of the error
        data_identifier -- identifier for the missing data
    """

    def __init__(self, message: str, data_identifier: str | None = None):
        self.message = message
        self.data_identifier = data_identifier
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Data Identifier: {self.data_identifier}]"


class DataWriteError(Exception):
    """Exception raised for errors during data writing operations.

    Attributes:
        message -- explanation of the error
        destination -- the target location where the write was attempted
    """

    def __init__(self, message: str, destination: str | None = None):
        self.message = message
        self.destination = destination
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Destination: {self.destination}]"


class DataReadError(Exception):
    """Exception raised for errors during data reading operations.

    Attributes:
        message -- explanation of the error
        source -- the source location from where the read was attempted
    """

    def __init__(self, message: str, source: str | None = None):
        self.message = message
        self.source = source
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Source: {self.source}]"


class DataTransformationError(Exception):
    """Exception raised for errors during data transformation operations.

    Attributes:
        message -- explanation of the error
        transformation -- the transformation step that failed
    """

    def __init__(self, message: str, transformation: str | None = None):
        self.message = message
        self.transformation = transformation
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Transformation: {self.transformation}]"


class DataConnectionError(Exception):
    """Exception raised for errors in data source or sink connections.

    Attributes:
        message -- explanation of the error
        endpoint -- the data source/sink endpoint that caused the error
    """

    def __init__(self, message: str, endpoint: str | None = None):
        self.message = message
        self.endpoint = endpoint
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Endpoint: {self.endpoint}]"


class SchemaMismatchError(Exception):
    """Exception raised when the data schema does not match the expected schema.

    Attributes:
        message -- explanation of the error
        expected_schema -- the schema that was expected
        actual_schema -- the schema that was found
    """

    def __init__(self, message: str, expected_schema: str | None = None, actual_schema: str | None = None):
        self.message = message
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Expected Schema: {self.expected_schema}] [Actual Schema: {self.actual_schema}]"


class DataIntegrityError(Exception):
    """Exception raised for data integrity violations.

    Attributes:
        message -- explanation of the error
        record_id -- identifier for the data record that violated integrity
    """

    def __init__(self, message: str, record_id: str | None = None):
        self.message = message
        self.record_id = record_id
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [Record ID: {self.record_id}]"
