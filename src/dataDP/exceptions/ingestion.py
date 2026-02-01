"""Custom exceptions for data ingestion processes."""


class VolumeIngestionError(Exception):
    """Exception raised for errors during the Unity Catalog ingestion process.

    Attributes:
        message -- explanation of the error
        url -- the source URL that failed
        destination -- the volume path where the file was supposed to go
    """

    def __init__(self, message: str, url: str | None = None, destination: str | None = None):
        self.message = message
        self.url = url
        self.destination = destination
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"{self.message} [URL: {self.url}] [Destination: {self.destination}]"
