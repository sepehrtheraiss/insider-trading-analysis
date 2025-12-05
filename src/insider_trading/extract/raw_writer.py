# raw_writer.py
import json
from pathlib import Path


class RawWriter:
    """Saves raw API responses for auditing and reproducibility."""

    RAW_DIR = Path("data/raw")

    def save(self, name, data):
        self.RAW_DIR.mkdir(parents=True, exist_ok=True)
        path = self.RAW_DIR / f"{name}.json"
        path.write_text(json.dumps(data, indent=2))
        return path
