# insider_cli.py
import click
from insider_trading.pipeline import InsiderTradingPipeline
from db.db import DB

@click.group()
def insider():
    pass

@insider.command()
def update():
    """Runs insider ETL pipeline."""
    db = DB()
    pipeline = InsiderTradingPipeline(db)
    pipeline.run()

if __name__ == "__main__":
    insider()
