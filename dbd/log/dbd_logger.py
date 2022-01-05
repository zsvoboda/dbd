import logging

FORMAT = "%(asctime)s %(levelname)s %(module)s '%(message)s'"


def setup_logging(level: int, filename: str):
    """
    Sets up the logging
    :param int level: logging level
    :param str filename: log filename
    """
    logging.basicConfig(level=level, format=FORMAT, filename=filename)
