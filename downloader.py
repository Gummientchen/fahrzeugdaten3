import requests
from pathlib import Path
from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_ERROR

def download_file(url: str, local_path: Path):
    """Downloads a file if it doesn't already exist locally."""
    if local_path.exists():
        log_message(LOG_LEVEL_INFO, f"File already exists: {local_path.resolve()}. Skipping download.")
        return
    
    log_message(LOG_LEVEL_INFO, f"Downloading {url} to {local_path.resolve()}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        log_message(LOG_LEVEL_INFO, f"Download complete for {local_path.name}.")
    except requests.exceptions.RequestException as e:
        log_message(LOG_LEVEL_ERROR, f"Downloading {url}: {e}")

def download_all_files(files_to_process: list, data_dir: Path):
    log_message(LOG_LEVEL_INFO, "\n--- Stage 1: Downloading files ---")
    for file_info in files_to_process:
        local_file_path = data_dir / file_info["local_name"]
        download_file(file_info["url"], local_file_path)