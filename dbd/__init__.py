from dbd.log.dbd_logger import setup_logging


class DbdException(Exception):
    """
    Top level exception
    """
    pass


setup_logging()
