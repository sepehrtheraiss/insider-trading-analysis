from ..base_api import BaseAPI
import sec_api

class SecApiAdapter(BaseAPI):
    """Library-based API client."""

    def __init__(self, api_key, proxy=None):
        super().__init__(api_key, proxy)
        self.sec_lib = sec_api
        self._client_cache = {}  # cache per class name
        self._method_cache = {}  # cache per class & method name

    def _get_client(self, client_class_name: str):
        """Return a cached lib-api client instance or create one if missing."""
        if client_class_name not in self._client_cache:
            client_class = getattr(self.sec_lib, client_class_name)
            self._client_cache[client_class_name] = client_class(self.api_key)
        return self._client_cache[client_class_name]
    
    def _get_method(self, client_class_name: str, method_name: str, client):
        """Return a cached bound method to its respective instance."""
        cache_key = (client_class_name, method_name)
        if cache_key not in self._method_cache:
            self._method_cache[cache_key] = getattr(client, method_name)
        return self._method_cache[cache_key]
    
    def fetch(self, client_class_name: str, method_name: str, *args, **kwargs):
        """
        Dynamically instantiate a specific lib client class and call its method.
        # 1. Locate the class in the lib
        # 2. Instantiate with api key
        # 3. Locate the method (like get_data / get_file)
        # 4. Call it        
        Example:
            fetch("InsiderTradingApi", "get_data", query={...})
            fetch("RenderApi", "get_file", url="url")
        """
        client = self._get_client(client_class_name)
        method = self._get_method(client_class_name, method_name, client)
        return method(*args, **kwargs)    