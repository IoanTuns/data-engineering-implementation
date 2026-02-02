import os
import subprocess

import requests
from loguru import logger


def upload_taxi_data_locally(
    year: int, month: int, taxi_type: str = "yellow", catalog: str = "workspace", schema: str = "taxi"
) -> None:
    """Downloads data to your laptop and uploads it to a Databricks Volume.
    Free Databricks accounts may have DNS issues on clusters, so this
    method uses your local internet connection to upload the data.
    """

    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    volume_path = f"/Volumes/{catalog}/{schema}/{taxi_type}/{file_name}"
    upload_data_locally(url, file_name, volume_path)


def upload_metadata_data_locally(catalog="workspace", schema="taxi") -> None:
    """Downloads data to your laptop and uploads it to a Databricks Volume.
    Free Databricks accounts may have DNS issues on clusters, so this
    method uses your local internet connection to upload the data.
    """

    reference_files = {
        "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        "taxi_zones.parquet": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
        "map_bronx.jpg": "https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_bronx.jpg",
        "map_brooklyn.jpg": "https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_brooklyn.jpg",
        "map_manhattan.jpg": "https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_manhattan.jpg",
        "map_queens.jpg": "https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_queens.jpg",
        "map_staten_island.jpg": "https://www.nyc.gov/assets/tlc/images/content/pages/about/taxi_zone_map_staten_island.jpg",
    }
    volume_path = f"/Volumes/{catalog}/{schema}/metadata"
    for file_name, url in reference_files.items():
        upload_data_locally(url, file_name, volume_path)


def upload_data_locally(url: str, file_name: str, volume_path: str) -> None:
    """Downloads data to your laptop and uploads it to a Databricks Volume.
    Free Databricks accounts may have DNS issues on clusters, so this
    method uses your local internet connection to upload the data.
    """
    local_path = os.path.join(os.getcwd(), file_name)

    try:
        # 1. Download to local machine
        logger.info(f"Downloading {file_name} locally...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # 2. Upload to Databricks Volume via CLI
        # This bypasses cluster DNS issues because it uses YOUR internet connection
        logger.info(f"Uploading to Databricks Volume: {volume_path}")
        result = subprocess.run(
            [
                "databricks",
                "fs",
                "cp",
                local_path,
                f"dbfs:{volume_path}",  # Volumes are accessible via dbfs: prefix in CLI
                "--overwrite",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        logger.success("Upload successful!")
        if result.returncode == 0:
            logger.success(f"Upload successful to {volume_path}")
        else:
            logger.error(f"Error during upload: {result.stderr}")

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Local file cleaned up.")


if __name__ == "__main__":
    """Upload taxi data to Databricks Volumes using local internet connection."""
    upload_taxi_data_locally(year=2025, month=11, taxi_type="yellow", catalog="workspace", schema="taxi")
    upload_taxi_data_locally(year=2025, month=10, taxi_type="yellow", catalog="workspace", schema="taxi")
    upload_taxi_data_locally(year=2025, month=9, taxi_type="yellow", catalog="workspace", schema="taxi")
    upload_taxi_data_locally(year=2025, month=11, taxi_type="green", catalog="workspace", schema="taxi")
    upload_taxi_data_locally(year=2025, month=10, taxi_type="green", catalog="workspace", schema="taxi")
    upload_taxi_data_locally(year=2025, month=9, taxi_type="green", catalog="workspace", schema="taxi")

    upload_metadata_data_locally(catalog="workspace", schema="taxi")
