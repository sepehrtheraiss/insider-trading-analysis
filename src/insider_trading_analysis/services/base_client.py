import requests

class ApiBaseClient:
    def __init__(self, key: str='', timeout: int=0):
        self.key= key 
        self.timeout = timeout
    

class HttpBaseClient:
    def __init__(self, base_url='', api_key=None, timeout=10):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"
    
    def request(self, method, endpoint, params=None, data=None, json=None, headers=None):
        url = self._url(endpoint)
        
        response = requests.request(
            method=method,
            url=url,
            params=params,
            data=data,
            json=json,
            headers=headers,
            timeout=self.timeout,
        )
        
        response.raise_for_status()
        return response.json()
    
    def get(self, endpoint, **kwargs):
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint, **kwargs):
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint, **kwargs):
        return self.request("PUT", endpoint, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self.request("DELETE", endpoint, **kwargs)