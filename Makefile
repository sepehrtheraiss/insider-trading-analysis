test:
	pytest -v --maxfail=1 --disable-warnings

coverage:
	pytest --cov=insider_trading --cov-report=term-missing

db-init:
	python -c "from db.db import init_db; init_db()"

load-exchange:
	python -m scripts.load_exchange_mapping

load-insider:
	python -m scripts.load_insider_transactions

load-ohlc:
	python -m scripts.load_ohlc_prices

