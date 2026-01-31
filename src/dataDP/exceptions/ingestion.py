"""Custom exceptions for data ingestion processes."""

class VolumeIngestionError(Exception):
    """Exception raised for errors during the Unity Catalog ingestion process.

    Attributes:
        message -- explanation of the error
        url -- the source URL that failed
        destination -- the volume path where the file was supposed to go
    """

    def __init__(self, message, url=None, destination=None):
        self.message = message
        self.url = url
        self.destination = destination
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message} [URL: {self.url}] [Destination: {self.destination}]"
