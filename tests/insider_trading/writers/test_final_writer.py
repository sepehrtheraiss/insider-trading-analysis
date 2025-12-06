import pandas as pd
import pytest
import time
from pathlib import Path
from writers.final_writer import FinalWriter


# ------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------

@pytest.fixture
def tmp_dir(tmp_path):
    """Temporary directory for isolated tests."""
    return tmp_path


@pytest.fixture
def schema():
    return [
        "issuerTicker",
        "cik",
        "exchange",
        "sector",
        "industry",
        "category",
        "name",
    ]


@pytest.fixture
def enforce_types():
    return {
        "issuerTicker": str,
        "cik": str,
        "exchange": str,
    }


@pytest.fixture
def writer(tmp_dir, schema, enforce_types):
    """FinalWriter configured for strict validation."""
    return FinalWriter(
        directory=tmp_dir,
        expected_schema=schema,
        enforce_types=enforce_types,
        keep_history=True,
    )


@pytest.fixture
def valid_df(schema):
    """A DataFrame matching expected schema and types."""
    return pd.DataFrame([
        {
            "issuerTicker": "AAPL",
            "cik": "0000320193",
            "exchange": "nasdaq",
            "sector": "tech",
            "industry": "hardware",
            "category": None,
            "name": "Apple Inc"
        }
    ])[schema]  # ensure correct column order


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

def test_final_writer_accepts_valid_dataframe(writer, valid_df, tmp_dir):
    """FinalWriter should accept correct schema and types."""
    path = writer.save("exchange_mapping_final", valid_df)

    assert path.exists()
    assert path.suffix == ".parquet"

    # Reload parquet and assert data preserved
    result = pd.read_parquet(path)
    assert result.equals(valid_df)


def test_final_writer_rejects_wrong_schema(writer, valid_df):
    """Ensure schema mismatch raises a ValueError."""
    broken_df = valid_df.rename(columns={"issuerTicker": "ticker"})

    with pytest.raises(ValueError) as exc:
        writer.save("exchange_mapping_final", broken_df)

    assert "Schema mismatch" in str(exc.value)


def test_final_writer_rejects_extra_columns(writer, valid_df):
    """FinalWriter should reject DataFrames with extra columns."""
    df = valid_df.copy()
    df["extra_col"] = 123  # invalid extra column

    with pytest.raises(ValueError):
        writer.save("exchange_mapping_final", df)


def test_final_writer_rejects_wrong_column_order(writer, valid_df, schema):
    """Column order matters — strict mode rejects reordering."""
    reordered = valid_df[schema[::-1]]  # reversed order

    with pytest.raises(ValueError):
        writer.save("exchange_mapping_final", reordered)


def test_final_writer_rejects_type_mismatch(writer, valid_df):
    """FinalWriter should enforce column types strictly."""
    df = valid_df.copy()
    df.loc[0, "issuerTicker"] = 123  # wrong type

    with pytest.raises(TypeError) as exc:
        writer.save("exchange_mapping_final", df)

    assert "wrong data types" in str(exc.value)


def test_final_writer_allows_nulls_in_valid_columns(writer, valid_df):
    """Nulls should be allowed, type-checking should ignore them."""
    df = valid_df.copy()
    df.loc[0, "issuerTicker"] = None  # valid

    # Should NOT raise
    path = writer.save("exchange_mapping_final", df)
    assert path.exists()


def test_final_writer_history_mode(writer, valid_df, tmp_dir):
    """keep_history=True should create timestamped files."""
    path1 = writer.save("exchange_mapping_final", valid_df)

    # Ensure next file is in a different second
    # Why 1.1 seconds?
    # Some OS clocks round timestamps — 1 second on the nose can still collide.
    time.sleep(1.1)
    path2 = writer.save("exchange_mapping_final", valid_df)

    assert path1.exists()
    assert path2.exists()
    assert path1 != path2  # timestamped → unique files


def test_final_writer_overwrite_mode(valid_df, tmp_dir, schema, enforce_types):
    """keep_history=False should overwrite same file name."""
    writer = FinalWriter(
        directory=tmp_dir,
        expected_schema=schema,
        enforce_types=enforce_types,
        keep_history=False,
    )

    path1 = writer.save("exchange_mapping_final", valid_df)
    path2 = writer.save("exchange_mapping_final", valid_df)

    assert path1 == path2  # same file overwritten
    assert path1.exists()

