# plot_cli.py
import click
from analytics.plot_service import PlotService
from db.db import DB

@click.command()
@click.argument("ticker")
@click.argument("start")
@click.argument("end")
def plot(ticker, start, end):
    """Plot insider vs OHLC."""
    db = DB()
    service = PlotService(db)
    service.plot(ticker, start, end)

if __name__ == "__main__":
    plot()
