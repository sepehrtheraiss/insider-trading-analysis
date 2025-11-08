from unittest.mock import ANY
import pytest
import itertools
import pandas as pd

from controllers.clean import (attach_mapping,
                               filter_valid_exchanges,
                               remove_price_outliers,
                               finalize)


def test_attach_mapping():
    # Arrange
    df = pd.DataFrame({
        "issuerTicker": ["AAPL", "MSFT", "GOOG"],
        "value": [10, 20, 30],
    })
    mapping = pd.DataFrame({
        "issuerTicker": ["AAPL", "GOOG"],
        "sector": ["Tech", "Search"]
    })

    # Act
    result = attach_mapping(df, mapping)

    # Assert
    assert "sector" in result.columns
    assert result.loc[result["issuerTicker"] == "AAPL", "sector"].iloc[0] == "Tech"
    assert pd.isna(result.loc[result["issuerTicker"] == "MSFT", "sector"]).iloc[0]


def test_filter_valid_exchanges():
    # Arrange
    df = pd.DataFrame({
        "issuerTicker": ["AAPL", "XYZ", "MSFT"],
        "exchange": ["NASDAQ", "OTC", "NYSE"],
    })

    # Act
    result = filter_valid_exchanges(df)

    # Assert
    assert set(result["exchange"].unique()) == {"NASDAQ", "NYSE"}
    assert "OTC" not in result["exchange"].values
    assert len(result) == 2


def test_remove_price_outliers():
    # Arrange
    df = pd.DataFrame({
        "issuerTicker": ["AAPL", "MSFT", "GOOG", "XYZ"],
        "isDelisted": [False, False, False, True],
        "pricePerShare": [150.0, 200000.0, None, 50.0]
    })

    # Act
    result = remove_price_outliers(df)

    # Assert
    # Only AAPL and XYZ should remain (XYZ is delisted so filtered out)
    assert all(result["pricePerShare"] < 100000)
    assert all(result["pricePerShare"] > 0)
    assert result["isDelisted"].eq(False).all()
    assert "pricePerShare" in result.columns


def test_finalize():
    # Arrange
    df = pd.DataFrame({
        "issuerTicker": ["AAPL", "MSFT"],
        "code": ["A1", None]
    })

    # Act
    result = finalize(df)

    # Assert
    assert "code_simple" in result.columns
    assert result.loc[result["issuerTicker"] == "AAPL", "code_simple"].iloc[0] == "A1"
    assert result.loc[result["issuerTicker"] == "MSFT", "code_simple"].iloc[0] == ""
    # ensure original not mutated
    assert "code_simple" not in df.columns
