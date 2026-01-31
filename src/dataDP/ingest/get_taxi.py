"""Handle ingestion of taxi data into Unity Catalog Volumes."""

from ingest_to_unity import ingest_to_unity_volume


def ingest_taxi_data(taxi_type, year, month, catalog, schema):
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    ingest_to_unity_volume(url, catalog, schema)
