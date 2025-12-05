from abc import ABC, abstractmethod

class BaseAPI(ABC):
    """Abstract base class for all API clients.
       proxy: mitmproxy for dev debugging
    """
    def __init__(self, api_key: str, proxy: str = None):
        self.api_key = api_key
        self.proxy = proxy

    def _configure(self):
        """Optional hook for subclass setup."""
        pass

    @abstractmethod
    def fetch(self, *args, **kwargs):
        """Perform an API operation."""
        ... 