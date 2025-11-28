from unittest.mock import ANY
import pytest
import itertools
import pandas as pd

def test_fetch_insider_transactions_called_with(sec_client, lib_fake_client):
    """Ensure get_transactions fetch calls correct funciton"""
    gen = sec_client.fetch_insider_transactions(query_string="",start="",end="",sleep_seconds=0)
    next(gen)
    lib_fake_client.fetch.assert_called_with("InsiderTradingApi", "get_data", ANY)

# def test_get_transactions_returns_data_sorted(sec_client, fake_client):
#     """Ensure get_transactions yields correct mocked transactions."""
#     gen = sec_client.fetch_insider_transactions(query_string="",start="",end="",sleep_seconds=0, sort_desc=True)
#     next(gen)
#     gen = sec_client.fetch_insider_transactions(query_string="",start="",end="",sleep_seconds=0, sort_desc=False)
#     next(gen)

@pytest.mark.parametrize("mock_response,expected_len", [
    ({"transactions": []}, 0),
    ({"transactions": [{"ticker": "TSLA"}]}, 1),
    ({"transactions": [{"ticker": "TSLA"}, {"ticker":"APPL"}]}, 2),
    (None, 0),
])
def test_fetch_insider_transactions_responses(sec_client, lib_fake_client, mock_response, expected_len):
    lib_fake_client.fetch.return_value = mock_response
    results = list(itertools.islice(sec_client.fetch_insider_transactions(query_string="",start="",end="", sleep_seconds=0),expected_len))
    assert len(results) == expected_len


def test_load_exchange_mapping(sec_client):
    res: pd.DataFrame = sec_client.fetch_exchange_mapping(exchanges=("nasdaq"))
    expected_columns = ["issuerTicker","cik","exchange","sector","industry","category","name"]
    assert all(col in res.columns for col in expected_columns)