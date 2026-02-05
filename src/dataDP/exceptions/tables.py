"""Module for defining custom exceptions related to table operations in DataDP."""


class TableCreationErrorException(Exception):
    """Exception raised when the creation of a table fails.

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
