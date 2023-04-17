import logging
import os
import re
from pathlib import Path


class Logger:
    """Helper class for logger setup."""

    def __init__(self, log_filepath: str = None, message_format: str = None, level: int = logging.INFO):
        """Initialize the Logger.

        Args:
            log_filepath (str): The file path where the log is written.
            message_format (str, optional): format of the log entry. Defaults to None.
        """
        if not message_format:
            message_format = '[%(name)s] >> [%(levelname)s] %(asctime)s >> %(message)s'

        logging.basicConfig(filename=log_filepath,
                            format=message_format,
                            level=level)

        self.logger = logging.getLogger('ETL')


def get_default_logger(log_filepath: str = None):
    log_filepath = None
    level = os.environ.get('LOG_LEVEL', 'INFO')

    if log_filepath:
        filepath_parent_directories = re.split('[\\\/]', log_filepath)[:-1]
        filepath_parent_directories = Path('/'.join(filepath_parent_directories))

        Path.mkdir(filepath_parent_directories, parents=True, exist_ok=True)

        logger = Logger(log_filepath, level=level)
    else:
        logger = Logger(level=level)

    return logger.logger
