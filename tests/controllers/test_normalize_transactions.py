import pandas as pd
import pytest
from typing import Any, Dict

# import the target function and helpers
from controllers.flatten import normalize_transactions, _extract_table_rows, _footnotes_text


def test_footnotes_text_returns_joined_text():
    footnotes = [{"text": "First"}, {"text": "Second"}]
    result = _footnotes_text(footnotes)
    assert result == "First\nSecond"

def test_footnotes_text_empty_or_invalid():
    assert _footnotes_text([]) == ""
    assert _footnotes_text(None) == ""
    # ignores non-dict items
    result = _footnotes_text([{"text": "Valid"}, "ignore"])
    assert "Valid" in result
    assert "ignore" not in result


def test_extract_table_rows_valid_and_invalid():
    filing = {"nonDerivativeTable": {"transactions": [{"id": 1}, {"id": 2}]}}
    rows = _extract_table_rows(filing, "nonDerivativeTable")
    assert isinstance(rows, list)
    assert len(rows) == 2
    # invalid type or missing key returns empty
    assert _extract_table_rows({}, "nonDerivativeTable") == []
    assert _extract_table_rows({"nonDerivativeTable": {"transactions": {}}}, "nonDerivativeTable") == []


@pytest.fixture
def sample_transaction() -> Dict[str, Any]:
    """Return a minimal valid Form 4-like structure."""
    return {
        "filedAt": "2024-03-10T10:00:00Z",
        "periodOfReport": "2024-03-09",
        "documentType": "4",
        "issuer": {"tradingSymbol": "AAPL", "cik": "0000320193", "name": "Apple Inc."},
        "reportingOwner": {
            "name": "Tim Cook",
            "cik": "0001234567",
            "relationship": {"isOfficer": True, "officerTitle": "CEO", "isDirector": False, "isTenPercentOwner": False},
        },
        "footnotes": [{"text": "Rule 10b5-1 plan in place"}],
        "nonDerivativeTable": {
            "transactions": [
                {
                    "coding": {"code": "P"},
                    "amounts": {"shares": "1000", "pricePerShare": "150", "acquiredDisposedCode": "A"},
                    "postTransactionAmounts": {"sharesOwnedFollowingTransaction": "5000"},
                    "transactionDate": "2024-03-08",
                }
            ]
        },
        "derivativeTable": {"transactions": []},
    }


def test_normalize_transactions_basic(sample_transaction):
    df = normalize_transactions([sample_transaction])
    # Shape check
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # Expected columns exist
    expected_cols = {
        "filedAt", "periodOfReport", "issuerTicker", "reporter",
        "shares", "pricePerShare", "totalValue", "is10b5_1"
    }
    assert expected_cols.issubset(df.columns)

    row = df.iloc[0]
    # Basic value integrity
    assert row["issuerTicker"] == "AAPL"
    assert row["reporter"] == "Tim Cook"
    assert row["shares"] == 1000
    assert row["pricePerShare"] == 150
    assert row["totalValue"] == 150000
    # Footnote text detection
    assert row["is10b5_1"] == True


def test_normalize_transactions_handles_empty_input():
    df = normalize_transactions([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_normalize_transactions_handles_invalid_numbers(sample_transaction):
    # break numbers to invalid strings
    sample_transaction["nonDerivativeTable"]["transactions"][0]["amounts"]["shares"] = "invalid"
    df = normalize_transactions([sample_transaction])
    assert pd.isna(df.loc[0, "shares"])
    assert pd.isna(df.loc[0, "totalValue"])
