import configparser

from util.logger.logger import get_default_logger


def read_app_config():
    config.read('app.ini')

    process_sleep_time = config.getint('settings', 'PROCESS_SLEEP_TIME')
    page_data_size_limit = config.getint('settings', 'PAGE_DATA_SIZE_LIMIT')

    configurations = {
        'PROCESS_SLEEP_TIME': process_sleep_time,
        'PAGE_DATA_SIZE_LIMIT': page_data_size_limit,
    }
    return configurations


LOGGER = get_default_logger()

config = configparser.ConfigParser()
