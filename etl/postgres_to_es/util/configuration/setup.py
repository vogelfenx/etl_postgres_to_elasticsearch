from util.logger import Logger
from pathlib import Path
import re
import os


def setup_logger():
    log_filepath = 'logs/.log'  # TODO: place it in config file
    log_filepath = None
    level = os.environ.get('LOG_LEVEL', 'INFO')

    if log_filepath:
        filepath_parent_directories = re.split('[\\\/]', log_filepath)[:-1]
        filepath_parent_directories = Path('/'.join(filepath_parent_directories))

        Path.mkdir(filepath_parent_directories, parents=True, exist_ok=True)

        Logger(log_filepath, level=level)
    else:
        Logger(level=level)


def setup():
    setup_logger()
