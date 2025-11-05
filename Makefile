test:
	pytest -v --maxfail=1 --disable-warnings

coverage:
	pytest --cov=src/insider_trading_analysis --cov-report=term-missing
