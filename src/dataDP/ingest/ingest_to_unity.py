"""Handle downloading files directly to Unity Catalog Volumes."""

import requests
import os

def ingest_to_unity_volume(url: str, catalog: str, schema: str, file_name: str, additional_path: str = None) -> None:
    """
    Downloads a file from a URL and saves it directly to a Unity Catalog Volume.

    Args:
        url (str): The direct download link for the file.
        catalog (str): The name of the Unity Catalog.
        schema (str): The name of the database/schema.
        file_name (str): The desired name for the saved file.
        additional_path (str, optional): Sub-folders within the volume. Defaults to "".

    Returns:
        None
    """
    # Construct the path using the Unity Catalog Volume standard format
    volume_path = f"/Volumes/{catalog}/{schema}"
    if additional_path:
        # Ensure sub-directories exist if specified
        volume_path = os.path.join(volume_path, additional_path.strip("/"))

    full_destination = os.path.join(volume_path, file_name)
    
    # Ensure the destination volume/directory is accessible
    if not os.path.exists(volume_path):
        print(f"Error: Volume path {volume_path} does not exist. Create it in UC first.")
        return

    print(f"Downloading {file_name} to {full_destination}...")
    
    # Streaming download: memory-efficient for large files
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(full_destination, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"File successfully saved to Unity Catalog Volume!")
    except Exception as e:
        print(f"An error occurred during download: {e}")