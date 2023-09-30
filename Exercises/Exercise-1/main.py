import os
import zipfile
import requests
import concurrent.futures
from shutil import rmtree
from urllib.parse import urlparse


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

default_downloads_folder = "./downloads"

class DownloadResult:
    def __init__(self, uri: str, files: list[str], e):
        self.uri = uri
        self.files = files
        self.exception = e

    def __eq__(self, other):
        if isinstance(other, DownloadResult):
            return self.uri == other.uri and \
            sorted(self.files) == sorted(other.files) and \
            str(self.exception) == str(other.exception)
        return False

    def __lt__(self, other):
        return self.uri < other.uri


def download_and_extract_zips(uris: list[str],
                              to: str="downloads",
                              workers: int=1) -> list[DownloadResult]:
    # Check if the "downloads" folder exists
    if os.path.exists(to):
        # If it exists, delete it and recreate it
        rmtree(to)
        os.mkdir(to)
        print(f"'{to}' folder cleared")
    else:
        # If it doesn't exist, create it
        os.mkdir(to)
        print(f"'{to}' folder created")

    # Array of dicts with input and either result or exception
    results: list[DownloadResult] = []
    
    # Create a ThreadPoolExecutor with 4 worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submit the tasks for execution
        futures = [executor.submit(download_and_extract_zip, uri, to) for uri in uris]
        future_to_uri = {}
        for future, uri in zip(futures, uris):
            future_to_uri[future] = uri

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                res = DownloadResult(future_to_uri[future], future.result(), None)
                results.append(res)
            except Exception as e:
                results.append(DownloadResult(future_to_uri[future], [], e))
                
    return results


def download_and_extract_zip(url: str, to: str) -> list[str]:
    # Validate if the URL is valid
    parsed_url = urlparse(url)
    if not (parsed_url.scheme and parsed_url.netloc):
        raise ValueError(f"URL {url} is not valid")

    # Extract the filename from the URL
    download_path = os.path.join(to, os.path.basename(parsed_url.path))

    # Download the ZIP
    response = requests.get(url, stream=True)

    response.raise_for_status()
    
    with open(download_path, "wb") as file:
        file.write(response.content)

    # Extract zip and return list of extracted files
    try:
        with zipfile.ZipFile(download_path, "r") as zip_ref:
            zip_ref.extractall(to)
            os.remove(download_path)
            return zip_ref.namelist()
    except Exception:
        os.remove(download_path)
        raise


def main():
    results = download_and_extract_zips(uris=download_uris,
                                                 to=default_downloads_folder,
                                                 workers=4)
    files = []
    [files.extend(result.files) for result in results]
    files_str = '\n'.join(files)
    print(f"Downloaded files:\n{files_str}")


if __name__ == "__main__":
    main()
