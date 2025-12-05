import os


SEC_API_KEY = os.getenv("SEC_API_KEY") or os.getenv("SECAPI_KEY") or os.getenv("SEC-API_KEY")
TIMEOUT = int(os.getenv("API_TIMEOUT", 10))
DEFAULT_PAGE_SIZE = int(os.getenv("PAGE_SIZE", 50))
BASE_URL = (os.getenv("BASE_URL", "https://api.sec-api.io")) 

class ConfigError(RuntimeError):
    pass

class InsiderTradingConfig:
    def __init__(self): ...

    @property
    def base_url(self) -> str:
        return BASE_URL
    @property
    def api_key(self) -> str:
        if not SEC_API_KEY:
            raise ConfigError("Missing SEC_API_KEY environment variable. Set it before running.")
        return SEC_API_KEY
    