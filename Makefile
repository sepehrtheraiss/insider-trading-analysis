test:
	pytest -v --maxfail=1 --disable-warnings

coverage:
	pytest --cov=src/insider_trading_analysis_etl --cov-report=term-missing

db-init:
	python -c "from insider_trading_analysis_etl.database.db import init_db; init_db()"

load-exchange:
	python -m insider_trading_analysis_etl.load.load_exchange_mapping

load-insider:
	python -m insider_trading_analysis_etl.load.load_insider_transactions

load-rollups:
	python -m insider_trading_analysis_etl.load.load_insider_rollups

load-ohlc:
	python -m insider_trading_analysis_etl.load.load_ohlc_prices

