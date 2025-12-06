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
    return tmp_path


@pytest.fixture
def schema():
    return [
        "issuer_ticker",
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
        "issuer_ticker": str,
        "cik": str,
        "exchange": str,
    }


@pytest.fixture
def writer(tmp_dir, schema, enforce_types):
    return FinalWriter(
        directory=tmp_dir,
        expected_schema=schema,
        enforce_types=enforce_types,
        keep_history=True,
    )


@pytest.fixture
def valid_df(schema):
    df = pd.DataFrame(
        [{
            "issuer_ticker": "AAPL",
            "cik": "0000320193",
            "exchange": "nasdaq",
            "sector": "tech",
            "industry": "hardware",
            "category": None,
            "name": "Apple Inc",
        }]
    )

    # Guarantee correct order
    return df[schema]


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

def test_final_writer_accepts_valid_dataframe(writer, valid_df):
    """FinalWriter should accept correct schema and types."""
    path = writer.save("exchange_mapping_final", valid_df)

    assert path.exists()
    assert path.suffix == ".parquet"

    # Read back and compare
    result = pd.read_parquet(path)
    assert list(result.columns) == list(valid_df.columns)
    assert result.equals(valid_df)


def test_final_writer_rejects_missing_columns(writer, valid_df):
    """Missing required schema columns should raise."""
    broken_df = valid_df.drop(columns=["issuer_ticker"])

    with pytest.raises(ValueError):
        writer.save("exchange_mapping_final", broken_df)


def test_final_writer_allows_extra_columns(writer, valid_df):
    """FinalWriter now allows extra columns — they are ignored."""
    df = valid_df.copy()
    df["extra_col"] = 123

    # Should NOT raise
    writer.save("exchange_mapping_final", df)


def test_final_writer_reorders_columns_automatically(writer, valid_df, schema):
    """FinalWriter should reorder columns to expected schema."""
    reordered = valid_df[schema[::-1]]  # reversed order

    # Should NOT raise now
    path = writer.save("exchange_mapping_final", reordered)

    result = pd.read_parquet(path)
    assert list(result.columns) == schema  # enforced canonical order


def test_final_writer_rejects_type_mismatch(writer, valid_df):
    """FinalWriter should enforce column types strictly."""
    df = valid_df.copy()
    df.loc[0, "issuer_ticker"] = 123  # wrong type

    with pytest.raises(TypeError):
        writer.save("exchange_mapping_final", df)


def test_final_writer_allows_nulls_in_valid_columns(writer, valid_df):
    df = valid_df.copy()
    df.loc[0, "issuer_ticker"] = None  # null allowed

    writer.save("exchange_mapping_final", df)


def test_final_writer_history_mode(writer, valid_df, tmp_dir):
    path1 = writer.save("exchange_mapping_final", valid_df)

    time.sleep(1.1)  # avoid timestamp collision
    path2 = writer.save("exchange_mapping_final", valid_df)

    assert path1.exists()
    assert path2.exists()
    assert path1 != path2


def test_final_writer_overwrite_mode(tmp_dir, schema, enforce_types, valid_df):
    writer = FinalWriter(
        directory=tmp_dir,
        expected_schema=schema,
        enforce_types=enforce_types,
        keep_history=False,
    )

    path1 = writer.save("exchange_mapping_final", valid_df)
    path2 = writer.save("exchange_mapping_final", valid_df)

    assert path1 == path2
    assert path1.exists()
