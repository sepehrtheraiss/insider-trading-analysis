import json
from pathlib import Path
from datetime import datetime, UTC


class RawWriter:
    """
    Writes raw API responses (Bronze layer).

    Key requirements:
      - Never overwrite raw data
      - Every saved file is timestamped
      - Stores human-readable JSON for debugging
      - Produces reproducible artifacts for auditing
    """

    def __init__(self, directory: str | Path = "data/raw"):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, data) -> Path:
        """
        Save raw API payload (Python dict or list) to a timestamped JSON file.

        Returns:
            Path to the written JSON file.
        """
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.json"

        path = self.dir / filename

        # Human-readable JSON for auditing / reproducibility
        path.write_text(json.dumps(data, indent=2))

        return path
