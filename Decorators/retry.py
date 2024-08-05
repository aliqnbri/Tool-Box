import time
from functools import wraps
from typing import Callable, Any, Type, Optional
from time import sleep
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry(retries: int = 3, delay: float = 1, exceptions: Optional[Type[BaseException]] = Exception) -> Callable:
    """
    Attempt to call a function, if it fails, try again with a specified delay.
    
    :param retries: The maximum number of retries for the function call
    :param delay: The delay (in seconds) between each retry
    :param exceptions: A tuple of exceptions to catch, defaults to Exception
    :return: The result of the function call if successful
    :raises: The last exception encountered if all retries fail
    """

    if retries < 1 or delay <= 0:
        raise ValueError('Retries must be at least 1 and delay must be greater than 0.')

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempt = 1
            while attempt <= retries:
                try:
                    logger.info(f'Attempt {attempt} for function {func.__name__}()')
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == retries:
                        logger.error(f'Function {func.__name__}() failed after {retries} retries with error: {e}')
                        raise
                    else:
                        logger.warning(f'Function {func.__name__}() attempt {attempt} failed with error: {e}. Retrying in {delay} seconds...')
                        time.sleep(delay)
                        attempt += 1
        return wrapper
    return decorator
