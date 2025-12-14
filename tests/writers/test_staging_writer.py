import pandas as pd
import pytest
from pathlib import Path
import time
from writers.staging_writer import StagingWriter


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

def test_staging_writer_creates_directory(tmp_path):
    writer = StagingWriter(directory=tmp_path / "silver")
    assert (tmp_path / "silver").exists()


def test_staging_writer_writes_parquet(tmp_path):
    writer = StagingWriter(directory=tmp_path)

    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = writer.save("cleaned_data", df)

    assert path.exists()
    assert path.suffix == ".parquet"


def test_staging_writer_rejects_non_dataframe(tmp_path):
    writer = StagingWriter(directory=tmp_path)

    with pytest.raises(TypeError):
        writer.save("bad_input", {"a": 1})


def test_staging_writer_timestamped_filename(tmp_path):
    writer = StagingWriter(directory=tmp_path)

    df = pd.DataFrame({"x": [1]})
    path = writer.save("staged_test", df)

    assert "staged_test_" in path.name
    assert path.name.endswith(".parquet")


def test_staging_writer_multiple_calls_do_not_overwrite(tmp_path):
    writer = StagingWriter(directory=tmp_path)

    df = pd.DataFrame({"x": [1]})
    p1 = writer.save("stage_test", df)
    time.sleep(1.1)
    p2 = writer.save("stage_test", df)

    assert p1.exists()
    assert p2.exists()
    assert p1 != p2


def test_staging_writer_coerces_object_to_string(tmp_path):
    writer = StagingWriter(directory=tmp_path)

    df = pd.DataFrame({"a": [1, 2], "b": ["x", None]})
    path = writer.save("stage_cast", df)

    import pyarrow.parquet as pq
    table = pq.read_table(path)

    # Parquet should store strings in a unified format
    assert str(table.schema.field("b").type) in ("string", "large_string")
