from util.logger import Logger
from pathlib import Path
import re
from logging import INFO, DEBUG


def setup_logger():
    log_filepath = 'logs/.log'  # TODO: place it in config file
    log_filepath = None
    level = DEBUG

    if log_filepath:
        filepath_parent_directories = re.split('[\\\/]', log_filepath)[:-1]
        filepath_parent_directories = Path('/'.join(filepath_parent_directories))

        Path.mkdir(filepath_parent_directories, parents=True, exist_ok=True)

        Logger(log_filepath, level=level)
    else:
        Logger(level=level)


def setup():
    setup_logger()
