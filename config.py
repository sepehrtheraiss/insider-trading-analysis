import os
from dotenv import load_dotenv

if os.path.exists('.env'):
    load_dotenv()
SEC_API_KEY = os.getenv("SEC_API_KEY") or os.getenv("SECAPI_KEY") or os.getenv("SEC-API_KEY")

class ConfigError(RuntimeError):
    pass

def require_api_key() -> str:
    if not SEC_API_KEY:
        raise ConfigError("Missing SEC_API_KEY environment variable. Set it before running.")
    return SEC_API_KEY
