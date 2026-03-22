"""Handle downloading files directly to Unity Catalog Volumes."""

import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dataDP import logger
from dataDP.decorators import with_logging
from dataDP.exceptions import VolumeIngestionError


@with_logging
def ingest_to_unity_volume(
    url: str, catalog: str, schema: str, file_name: str, additional_path: str | None = None
) -> None:
    """
    Downloads a file from a URL and saves it directly to a Unity Catalog Volume.

    Args:
        url (str): The direct download link for the file.
        catalog (str): The name of the Unity Catalog.
        schema (str): The name of the database/schema.
        file_name (str): The desired name for the saved file.
        additional_path (str, optional): Sub-folders within the volume. Defaults to None".

    Returns:
        None
    """
    # Construct the path using the Unity Catalog Volume standard format
    volume_path = f"/Volumes/{catalog}/{schema}"
    ingest_to_data_from_api(volume_path, url, catalog, schema, file_name, additional_path)


@with_logging
def ingest_to_data_from_api(
    volume_path: str,
    url: str,
    catalog: str,
    schema: str,
    file_name: str,
    additional_path: str | None = None,
    force_download: bool = False,
) -> None:
    """
    Downloads a file from a URL and saves it into desire location.

    Args:
        volume_path (str): The direct download link for the file.
        url (str): The direct download link for the file.
        catalog (str): The name of the Unity Catalog.
        schema (str): The name of the database/schema.
        file_name (str): The desired name for the saved file.
        additional_path (str, optional): Sub-folders within the volume. Defaults to None.

    Returns:
        None
    """
    timeout = 10  # seconds
    retry = 5  # number of retries

    if additional_path:
        # Ensure sub-directories exist if specified
        volume_path = os.path.join(volume_path, additional_path.strip("/"))

    full_destination = os.path.join(volume_path, file_name)

    # Skip download if file already exists and force_download is False
    if os.path.exists(full_destination) and not force_download:
        logger.info(f"File {full_destination} already exists. Skipping download.")
        return

    # Ensure the destination volume/directory is accessible
    if not os.path.exists(volume_path):
        msg = f"Volume path {volume_path} does not exist."
        logger.error(msg)
        raise FileNotFoundError(msg)
    elif not os.access(volume_path, os.W_OK):
        msg = f"No write access to volume path {volume_path}."
        logger.error(msg)
        raise PermissionError(msg)

    logger.info(f"Downloading {file_name} to {full_destination}...")

    # Streaming download: memory-efficient for large files
    # use requests parameters are in actual impementation scope
    # if needed, add headers, auth, timeout, etc.
    session = requests.Session()
    retries = Retry(total=retry, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(full_destination, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info("File successfully saved to Unity Catalog Volume!")
    except Exception as e:
        msg = "Failed to download or save the file to Unity Catalog Volume."
        logger.error(f"{msg} URL: {url}, Destination: {full_destination}", exc_info=True)
        raise VolumeIngestionError(message=msg, url=url, destination=full_destination) from e
    except requests.exceptions.RequestException as e:
        msg = "HTTP error occurred during file download."
        logger.error(f"{msg} URL: {url}", exc_info=True)
        raise VolumeIngestionError(message=msg, url=url) from e
    except Exception as e:
        msg = "An unexpected error occurred during file download."
        logger.error(f"{msg} URL: {url}", exc_info=True)
        raise VolumeIngestionError(message=msg) from e
