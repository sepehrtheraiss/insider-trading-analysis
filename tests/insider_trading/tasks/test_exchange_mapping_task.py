import pandas as pd
import pytest
from unittest.mock import MagicMock

from insider_trading.tasks.exchange_mapping_task import ExchangeMappingTask


# ------------------------------------------------------------------------
# Fixtures (Pytest auto-injects these into tests)
# ------------------------------------------------------------------------

@pytest.fixture
def fake_raw_data():
    return [
        {"ticker": "AAPL", "exchange": "nasdaq"},
        {"ticker": "MSFT", "exchange": "nasdaq"},
    ]


@pytest.fixture
def fake_df_final():
    return pd.DataFrame([
        {
            "issuerTicker": "AAPL",
            "cik": "0000320193",
            "exchange": "nasdaq",
            "sector": "tech",
            "industry": "software",
            "category": None,
            "name": "Apple Inc"
        },
        {
            "issuerTicker": "MSFT",
            "cik": "0000789019",
            "exchange": "nasdaq",
            "sector": "tech",
            "industry": "software",
            "category": None,
            "name": "Microsoft Corp"
        }
    ])


@pytest.fixture
def mock_source(fake_raw_data):
    mock = MagicMock()
    mock.fetch_exchange_mapping.return_value = fake_raw_data
    return mock


@pytest.fixture
def mock_transformer(fake_df_final):
    mock = MagicMock()
    mock.transform.return_value = fake_df_final
    return mock


@pytest.fixture
def mock_loader():
    mock = MagicMock()
    return mock


@pytest.fixture
def mock_raw_writer():
    mock = MagicMock()
    mock.save.return_value = "data/raw/exchange_mapping_test.json"
    return mock


@pytest.fixture
def mock_staging_writer():
    mock = MagicMock()
    mock.save.return_value = "data/staging/exchange_mapping_step.parquet"
    return mock


@pytest.fixture
def mock_final_writer():
    mock = MagicMock()
    mock.save.return_value = "data/final/exchange_mapping_final.parquet"
    return mock


@pytest.fixture
def task(
    mock_source,
    mock_transformer,
    mock_loader,
    mock_raw_writer,
    mock_staging_writer,
    mock_final_writer,
):
    return ExchangeMappingTask(
        source=mock_source,
        transformer=mock_transformer,
        loader=mock_loader,
        raw_writer=mock_raw_writer,
        staging_writer=mock_staging_writer,
        final_writer=mock_final_writer,
    )


# ------------------------------------------------------------------------
# TESTS
# ------------------------------------------------------------------------

def test_exchange_mapping_task_runs_full_flow(
    task,
    mock_source,
    mock_transformer,
    mock_loader,
    mock_raw_writer,
    mock_staging_writer,
    mock_final_writer,
    fake_raw_data,
    fake_df_final
):
    """Full happy-path test ensuring raw → transform → staging → final → load."""
    task.run()

    # 1. Extract was called
    mock_source.fetch_exchange_mapping.assert_called_once()

    # 2. Raw writer saved the original data
    mock_raw_writer.save.assert_called_once_with("exchange_mapping", fake_raw_data)

    # 3. Transformer received raw data
    mock_transformer.transform.assert_called_once()

    # 4. Staging writer used inside transformer (we assume transformer calls it)
    # Not verifying here because it's inside transformer, not task.
    # But you CAN check transform() was passed the writer:
    call_args = mock_transformer.transform.call_args.kwargs
    assert "staging_writer" in call_args

    # 5. Final writer was called with the final DF
    mock_final_writer.save.assert_called_once()
    assert mock_final_writer.save.call_args[0][0] == "exchange_mapping_final"
    assert mock_final_writer.save.call_args[0][1].equals(fake_df_final)

    # 6. Loader received the final DF
    mock_loader.load.assert_called_once_with(fake_df_final)


# ------------------------------------------------------------------------
# OPTIONAL: Failure Case Example
# ------------------------------------------------------------------------

def test_exchange_mapping_task_transform_failure(task, mock_transformer):
    """If transformer raises an exception, the task should not call loader."""
    mock_transformer.transform.side_effect = Exception("Transform failed")

    with pytest.raises(Exception):
        task.run()

    task.loader.load.assert_not_called()

