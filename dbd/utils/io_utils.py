from requests import get as get_url


def is_url(url: str) -> bool:
    """
    Returns True if the string is a URL
    :param str url: string to check
    :return: True if the string is a URL
    :rtype: bool
    """
    return url.startswith('http') or url.startswith('ftp')


def url_to_filename(url: str) -> str:
    """
    Returns the filename from a URL
    :param str url: URL
    :return: filename
    :rtype: str
    """
    return url.split('?')[0].split('/')[-1]


def download_file(url, local_filename: str):
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
