from functools import wraps
from time import sleep
import logging
from typing import Callable


def backoff(
    *,
    start_sleep_time: float = 0.1,
    factor: int = 2,
    border_sleep_time: int = 10,
) -> Callable:
    """
    Re-execute Function after some time if an error has occurred.

    Uses a naive exponential growth of the retry time (factor) to the border_sleep_time.

    Args:
        start_sleep_time (float):
            Initial repeat time. Defaults to 0.1.
        factor (int):
            The number of times that the waiting time has to be increased by.
            Defaults to 2.
        border_sleep_time (int):
            Time limit for the backoff process to be reset. Defaults to 10.

    Returns:
        Callable: an wrapped function.
    """
    def function_decorator(function: Callable) -> Callable:

        @wraps(function)
        def function_wrapper(*args, **kwargs):
            num_calls = 0
            sleep_time = start_sleep_time

            num_calls += 1
            logging.debug(
                'Backoff function try %s: %s. time',
                function_wrapper.__name__,
                num_calls,
            )
            while True:
                try:
                    return function(*args, **kwargs)
                except Exception:
                    sleep_time *= factor
                    if sleep_time >= border_sleep_time:
                        sleep_time = start_sleep_time
                    logging.error(
                        'Function call %s() failed. Next try in %s seconds',
                        function_wrapper.__name__,
                        sleep_time,
                    )
                    sleep(sleep_time)

        return function_wrapper

    return function_decorator
