from functools import wraps
from time import sleep
from util.configuration import LOGGER
from typing import Callable


# здорово, что сделал кастомную реализацию, молодец
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
            LOGGER.debug(
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
                        # возможно, я неправильно понял
                        # но, как мне кажется sleep_time тут должен был
                        # стать равным border_sleep_time
                        sleep_time = start_sleep_time
                    LOGGER.error(
                        'Function call %s() failed. Next try in %s seconds',
                        function_wrapper.__name__,
                        sleep_time,
                    )
                    sleep(sleep_time)

        return function_wrapper

    return function_decorator
