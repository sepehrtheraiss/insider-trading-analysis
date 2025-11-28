import time
import logging
from functools import wraps

def backoff_retry(max_retries=3, backoff_factor=1.5, exceptions=(Exception,)):
    """Exponential backoff retry decorator."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise
                    sleep_time = backoff_factor ** attempt
                    logging.warning(f"{func.__name__} failed ({e}); retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
        return wrapper
    return decorator
