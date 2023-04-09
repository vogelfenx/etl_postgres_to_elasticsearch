import logging

from util.configuration import setup

if __name__ == '__main__':
    setup()
    logging.error('%s', 'test message')
