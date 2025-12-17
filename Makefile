PYTHON := python3
test:
	pytest -v --maxfail=1 --disable-warnings

coverage:
	pytest --cov=insider_trading --cov-report=term-missing

db-init:
	$(PYTHON) -c "from db.db import init_db; init_db()"

load-exchange:
	$(PYTHON) -m scripts.load_exchange_mapping

load-insider:
	$(PYTHON) -m scripts.load_insider_transactions

load-ohlc:
	$(PYTHON) -m scripts.load_ohlc_prices

