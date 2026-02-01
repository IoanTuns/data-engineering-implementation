"""Handle ingestion of taxi data into Unity Catalog Volumes."""

from dataDP import logger
from dataDP.decorators import with_logging
from dataDP.ingest import ingest_to_unity_volume


@with_logging
def ingest_ny_taxi_data(taxi_type, year, month, catalog, schema):
    """Ingest New York taxi data into Unity Catalog Volume.
    Args:
        taxi_type (str): Type of taxi data ('yellow', 'green', etc.).
        year (int): Year of the data.
        month (int): Month of the data.
        catalog (str): Unity Catalog name.
        schema (str): Database/schema name.
    """
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    logger.info(f"Ingesting {file_name}...")
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    ingest_to_unity_volume(url, catalog, schema)
