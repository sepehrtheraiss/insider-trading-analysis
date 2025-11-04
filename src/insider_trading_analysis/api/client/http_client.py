import requests
from ..base_api import BaseAPI
from .decorators.ratelimiter import rate_limited
from .decorators.backoff import backoff_retry

class HttpClient(BaseAPI):
    """HTTP API client with rate limit & retry decorators."""

    def __init__(self, base_url, token=None, proxy=None, rate=30, per=60):
        super().__init__(token, proxy)
        self.base_url = base_url.rstrip('/')
        self.rate = rate
        self.per = per
        self.session = None
        self.token = self.api_key
        self._configure()

    def _configure(self):
        self.session = requests.Session()
        if self.token:
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        return self.session

    @rate_limited(rate=30, per=60)
    @backoff_retry(max_retries=4, backoff_factor=2.0, exceptions=(requests.exceptions.RequestException,))
    def fetch(self, endpoint, params=None, method="GET"):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.request(method, url, params=params, timeout=10)
        # handle explicit 429 as retryable
        if response.status_code == 429:
            class RateLimitError(Exception): ...
            err = RateLimitError("429 Too Many Requests")
            err.response = response
            raise err
        response.raise_for_status()
        return response.json()

    def fetch_pages(self, endpoint: str, page_param: str = "page", limit: int = 100, max_pages: int | None = None):
        """Simple page-based pagination helper that accumulates 'data' items."""
        results = []
        page = 1
        while True:
            payload = self.fetch(endpoint, params={page_param: page, "limit": limit})
            data = payload.get("data", payload if isinstance(payload, list) else [])
            if not data:
                break
            results.extend(data)
            page += 1
            if max_pages and page > max_pages:
                break
        return results

