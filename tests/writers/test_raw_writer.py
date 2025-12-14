import json
import re
import time
from pathlib import Path

import pytest

from writers.raw_writer import RawWriter


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def is_timestamped(filename: str):
    """
    Check the filename contains a timestamp like: _YYYYMMDD_HHMMSS.json
    """
    pattern = r".*_\d{8}_\d{6}\.json$"
    return re.match(pattern, filename) is not None


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

def test_raw_writer_creates_directory_if_missing(tmp_path):
    """RawWriter should automatically create the target directory."""
    raw_dir = tmp_path / "raw_data"
    writer = RawWriter(directory=raw_dir)

    assert raw_dir.exists()
    assert raw_dir.is_dir()


def test_raw_writer_writes_json_file(tmp_path):
    """Saving raw payload should create a timestamped JSON file."""
    writer = RawWriter(directory=tmp_path)

    payload = {"foo": "bar", "value": 123}
    path = writer.save("insider_transactions", payload)

    assert path.exists()
    assert path.suffix == ".json"
    assert is_timestamped(path.name)


def test_raw_writer_json_content_is_correct(tmp_path):
    """File contents should match the JSON payload written."""
    writer = RawWriter(directory=tmp_path)
    payload = {"a": 1, "b": 2, "c": [1, 2, 3]}

    path = writer.save("test_payload", payload)

    with path.open() as f:
        data = json.load(f)

    assert data == payload


def test_raw_writer_writes_list_payload(tmp_path):
    """RawWriter should handle list-of-dicts as well."""
    writer = RawWriter(directory=tmp_path)
    payload = [{"x": 1}, {"y": 2}]

    path = writer.save("list_payload", payload)

    assert path.exists()
    assert is_timestamped(path.name)

    with path.open() as f:
        data = json.load(f)

    assert data == payload


def test_raw_writer_does_not_overwrite(tmp_path):
    """Two consecutive saves should create two different files."""
    writer = RawWriter(directory=tmp_path)
    payload = {"k": "v"}

    path1 = writer.save("dup_test", payload)
    time.sleep(1.1)  # ensure timestamp differs
    path2 = writer.save("dup_test", payload)

    assert path1.exists()
    assert path2.exists()
    assert path1 != path2


def test_raw_writer_returns_correct_path_type(tmp_path):
    """Writer.save() must return a pathlib.Path instance."""
    writer = RawWriter(directory=tmp_path)
    payload = {"hello": "world"}

    path = writer.save("valid", payload)

    assert isinstance(path, Path)
    assert path.exists()
