import logging

from util.configuration import setup

if __name__ == '__main__':
    setup()
    # test logging
    logging.debug('%s', 'start etl process')
