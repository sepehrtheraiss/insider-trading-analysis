import time
from threading import Lock
from functools import wraps

class RateLimiter:
    """Client-side token-bucket rate limiter."""
    def __init__(self, rate: int, per: int):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.time()
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            current = time.time()
            elapsed = current - self.last_check
            self.last_check = current
            self.allowance += elapsed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate

            if self.allowance < 1.0:
                sleep_time = (1.0 - self.allowance) * (self.per / self.rate)
                time.sleep(sleep_time)
                self.allowance = 0
            else:
                self.allowance -= 1.0

def rate_limited(rate: int, per: int):
    """Decorator for rate-limiting methods."""
    limiter = RateLimiter(rate, per)
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter.acquire()
            return func(*args, **kwargs)
        return wrapper
    return decorator
