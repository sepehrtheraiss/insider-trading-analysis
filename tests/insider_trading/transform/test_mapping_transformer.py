import pandas as pd
import pytest

from insider_trading.transform.mapping_transformer import MappingTransformer


@pytest.fixture
def transformer():
    return MappingTransformer()


def test_normalize(transformer):
    raw = [{"ticker": "AAPL", "cik": "0000320193"}]
    df = transformer.normalize(raw)

    assert isinstance(df, pd.DataFrame)
    assert df.loc[0, "ticker"] == "AAPL"
    assert df.loc[0, "cik"] == "0000320193"


def test_clean_handles_empty_df(transformer):
    df = pd.DataFrame()
    cleaned = transformer.clean(df)

    assert cleaned.empty
    assert list(cleaned.columns) == transformer.SCHEMA


def test_clean_renames_and_adds_missing_columns(transformer):
    df = pd.DataFrame([{"ticker": "AAPL", "exchange": "nasdaq"}])
    cleaned = transformer.clean(df)

    assert "issuer_ticker" in cleaned.columns
    assert cleaned.loc[0, "issuer_ticker"] == "AAPL"

    # Should contain all required SCHEMA columns
    assert list(cleaned.columns) == transformer.SCHEMA

    # Missing values should be None/NaN
    assert pd.isna(cleaned.loc[0, "sector"])


def test_dedupe_prefers_nasdaq_over_nyse(transformer):
    df = pd.DataFrame([
        {
            "issuer_ticker": "MSFT",
            "exchange": "nyse",
            "cik": "abc",
            "sector": "tech",
            "industry": None,
            "category": None,
            "name": "MSFT"
        },
        {
            "issuer_ticker": "MSFT",
            "exchange": "nasdaq",
            "cik": "abc",
            "sector": "tech",
            "industry": None,
            "category": None,
            "name": "MSFT"
        },
    ])

    deduped = transformer.dedupe(df)

    assert len(deduped) == 1
    assert deduped.iloc[0]["exchange"] == "nasdaq"


def test_transform_full_pipeline(transformer):
    raw = [
        {
            "ticker": "AAPL",
            "cik": "0000320193",
            "exchange": "nasdaq",
            "sector": "tech",
            "industry": "hardware",
            "category": "large",
            "name": "Apple Inc"
        }
    ]

    df = transformer.transform(raw)

    assert list(df.columns) == transformer.SCHEMA
    assert df.loc[0, "issuer_ticker"] == "AAPL"
    assert len(df) == 1
