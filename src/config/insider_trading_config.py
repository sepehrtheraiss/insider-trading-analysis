import os


SEC_API_KEY = os.getenv("SEC_API_KEY") or os.getenv("SECAPI_KEY") or os.getenv("SEC-API_KEY")
TIMEOUT = int(os.getenv("API_TIMEOUT", 10))
DEFAULT_PAGE_SIZE = int(os.getenv("PAGE_SIZE", 50))
BASE_URL = (os.getenv("BASE_URL", "https://api.sec-api.io")) 
TEST_MODE_TX = os.getenv("TEST_MODE_TX", "").lower() in ("1", "true", "yes", "y")
TEST_MODE_MAP= os.getenv("TEST_MODE_MAP", "").lower() in ("1", "true", "yes", "y")
TEST_PATH_TX= os.getenv("TEST_PATH_TX")
TEST_PATH_MAP= os.getenv("TEST_PATH_MAP")

class ConfigError(RuntimeError):
    pass

class InsiderTradingConfig:

    @property
    def test_mode_map(self):
        if not TEST_MODE_MAP:
            raise ConfigError("Missing TEST_MODE_MAP environment variable. Set it before running.")
        return TEST_MODE_MAP

    @property
    def test_mode_tx(self):
        if not TEST_MODE_TX:
            raise ConfigError("Missing TEST_MODE_TX environment variable. Set it before running.")
        return TEST_MODE_TX

    @test_mode_tx.setter
    def test_mode_tx(self, value: bool):
        self.TEST_MODE_TX = value

    @property
    def test_path_tx(self):
        if not TEST_PATH_TX:
            raise ConfigError("Missing TEST_PATH_TX environment variable. Set it before running.")
        return TEST_PATH_TX

    @test_path_tx.setter
    def test_path_tx(self, value: str):
        self.TEST_PATH_TX= value

    @property
    def test_path_map(self):
        if not TEST_PATH_MAP:
            raise ConfigError("Missing TEST_PATH_MAP environment variable. Set it before running.")
        return TEST_PATH_MAP

    @property
    def base_url(self) -> str:
        return BASE_URL

    @property
    def api_key(self) -> str:
        if not SEC_API_KEY:
            raise ConfigError("Missing SEC_API_KEY environment variable. Set it before running.")
        return SEC_API_KEY
    