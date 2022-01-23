from typing import List, Tuple
from zipfile import ZipFile

from kaggle import KaggleApi
from requests import get as get_url


class DbdInvalidRef(Exception):
    pass


ZIP_LOCATOR_SEPARATOR = '>'


def is_url(url: str) -> bool:
    """
    Returns True if the string is a URL
    :param str url: string to check
    :return: True if the string is a URL
    :rtype: bool
    """
    return url.lower().startswith('http') or url.lower().startswith('ftp')


def is_zip(url: str) -> bool:
    """
    Returns True if the string is a ZIP file locator
    :param str url: string to check
    :return: True if the string is a URL
    :rtype: bool
    """
    return ZIP_LOCATOR_SEPARATOR in url.lower()


def is_kaggle(url: str) -> bool:
    """
    Returns True if the string is a Kaggle dataset URL
    :param str url: string to check
    :return: True if the string is a Kaggle dataset URL
    :rtype: bool
    """
    return url.lower().startswith('kaggle://')


def extract_kaggle_dataset_id_and_zip_name(url: str) -> Tuple[str, str]:
    """
    Returns the Kaggle dataset id from dataset URL
    :param str url: string to check
    :return: Returns the Kaggle dataset id
    :rtype: str
    """
    kaggle_dataset_id = url.split('kaggle://')[-1]
    kaggle_zip_filename = kaggle_dataset_id.split('/')[-1]
    return kaggle_dataset_id, kaggle_zip_filename


def url_to_filename(url: str) -> str:
    """
    Returns the filename from a URL
    :param str url: URL
    :return: filename
    :rtype: str
    """
    return url.split('?')[0].split('/')[-1]


def zip_to_url_and_locator(url: str) -> List[str]:
    """
    Returns the filename from a ZIP file locator
    :param str url: ZIP file locator
    :return: filename
    :rtype: List[str]
    """
    components = url.split(ZIP_LOCATOR_SEPARATOR)
    if len(components) != 2:
        raise DbdInvalidRef(f"Invalid ZIP file locator: '{url}'")
    return components


def download_file(url: str, local_filename: str):
    """
    Downloads a file from a URL to a local file
    :param str url: url of the file to download
    :param str local_filename: local filename to save the file to
    :return: None 
    """
    with get_url(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_kaggle(dataset_id: str, tmpdir: str):
    """
    Downloads a file from Kaggle to tmpdir
    :param str dataset_id: Kaggle dataset id
    :param str tmpdir: dir to save the file to
    :return: None
    """
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_id, tmpdir, unzip=False, force=False)


def extract_zip_file(zip_file: str, zip_locator: str, local_filename: str):
    """
    Downloads a file from a ZIP file locator to a local file
    :param str zip_file: ZIP file
    :param str zip_locator: ZIP file locator
    :param str local_filename: local filename to save the file to
    :return: None
    """
    with ZipFile(zip_file, 'r') as zipObj:
        zipObj.extract(zip_locator, local_filename)
