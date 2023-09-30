import os
import io
import unittest
import zipfile
import tempfile
import requests
import requests_mock
from main import download_and_extract_zips, DownloadResult

@requests_mock.Mocker()
class TestDownloadAndExtractZips(unittest.TestCase):
    def test_success_single_file(self, m):
        url = "http://test.com/nice.zip"
        file_name = "some_file.txt"
        content = "Very important information."
        want_download_results = [
            DownloadResult(url, [file_name], None)
        ]

        # Download zip bytes on get
        file_bytes = create_mock_zip_file(file_name, content)
        m.get(url, content=file_bytes)

        # Save it to random temporary directory, it returns name of file from zip
        dir = tempfile.mkdtemp()
        download_results = download_and_extract_zips([url], dir)
        self.assertListEqual(want_download_results, sorted(download_results))

        # Correct downloaded file content
        path = os.path.join(dir, "some_file.txt")
        with open(path, "r") as downloaded_file:
            downloaded_content = downloaded_file.read()
            self.assertEqual(content, str(downloaded_content))
        # There is nothing else in this download dir
        self.assertEqual([file_name], os.listdir(dir))


    def test_success_many_files(self, m):
        urls = ["http://test.com/nice1.zip", "http://test.com/nice2.zip"]
        file_names = ["simple_file_1.txt", "simple_file_2.txt"]
        contents = ["Very important information 1.","Very important information 2."]
        want_download_results = [
            DownloadResult(urls[0], [file_names[0]], None),
            DownloadResult(urls[1], [file_names[1]], None)
        ]

        file_bytes1 = create_mock_zip_file(file_names[0], contents[0])
        file_bytes2 = create_mock_zip_file(file_names[1], contents[1])
        m.get(urls[0], content=file_bytes1)
        m.get(urls[1], content=file_bytes2)

        dir = tempfile.mkdtemp()
        download_results = download_and_extract_zips(urls, dir, 2)
        self.assertEqual(sorted(want_download_results), sorted(download_results))

        for i, file_name in enumerate(file_names):
            path = os.path.join(dir, file_name)
            with open(path, "r") as downloaded_file:
                downloaded_content = downloaded_file.read()
                self.assertEqual(contents[i], str(downloaded_content))
        self.assertListEqual(file_names, sorted(os.listdir(dir)))


    def test_invalid_url(self, m):
        urls = ["http://test.com/nice1.zip", "not-reall-an-url"]
        file_name = "some_file.txt"
        content = "Very important information."

        file_bytes = create_mock_zip_file("some_file.txt", content)
        m.get(urls[0], content=file_bytes)

        dir = tempfile.mkdtemp()
        download_and_extract_zips(urls, dir)
        [
            DownloadResult(urls[0], [file_name], None),
            DownloadResult(urls[1], [], ValueError("URL not-reall-an-urlis not valid"))
        ]

        path = os.path.join(dir, "some_file.txt")
        with open(path, "r") as downloaded_file:
            downloaded_content = downloaded_file.read()
            self.assertEqual(content, str(downloaded_content))
        self.assertEqual([file_name], os.listdir(dir))


    def test_fail_file_not_found(self, m):
        url = "http://test.com/nice.zip"
        want_download_results = [
            DownloadResult(url, [], requests.HTTPError("404 Client Error: None for url: http://test.com/nice.zip"))
        ]

        m.get(url, status_code=404)

        dir = tempfile.mkdtemp()
        download_results = download_and_extract_zips([url], dir)
        self.assertListEqual(want_download_results, sorted(download_results))
        self.assertEqual([], os.listdir(dir))


    def test_fail_download_error(self, m):
        url = "http://test.com/nice.zip"
        want_download_results = [
            DownloadResult(url, [], requests.HTTPError("something went wrong :("))
        ]

        m.get(url, exc=requests.HTTPError("something went wrong :("))

        dir = tempfile.mkdtemp()
        download_results = download_and_extract_zips([url], dir)
        self.assertListEqual(want_download_results, sorted(download_results))
        self.assertEqual([], os.listdir(dir))

        
    def test_fail_file_is_not_zip(self, m):
        url = "http://test.com/nice.zip"
        want_download_results = [
            DownloadResult(url, [], zipfile.BadZipFile("File is not a zip file"))
        ]

        # Downloaded text file instead of zip
        m.get(url, content=b"Text file content")

        dir = tempfile.mkdtemp()
        download_results = download_and_extract_zips([url], dir)
        self.assertListEqual(want_download_results, sorted(download_results))
        self.assertEqual([], os.listdir(dir))


def create_mock_zip_file(filename: str, content: str) -> bytes:
    bytes_content = io.BytesIO()
    with zipfile.ZipFile(bytes_content, 'w') as mock_zip:
        mock_zip.writestr(filename, content)
    bytes_content.seek(0)
    return bytes_content.read()