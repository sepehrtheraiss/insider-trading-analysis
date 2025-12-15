import pandas as pd

def total_sec_acq_dis_day(df: pd.DataFrame) -> pd.DataFrame:
    """Total $ acquired and disposed per day."""
    acquired = (
        df[df["acquired_disposed"] == "A"]
        .groupby("period_of_report")["total_value"]
        .sum()
        .rename("acquired")
    )

    disposed = (
        df[df["acquired_disposed"] == "D"]
        .groupby("period_of_report")["total_value"]
        .sum()
        .rename("disposed")
    )

    out = pd.concat([acquired, disposed], axis=1).fillna(0)
    return out.sort_values("period_of_report")

def companies_bs_in_period(df: pd.DataFrame, start, end):
    """Top companies bought/sold in a given year."""
    mask = (
        (df['period_of_report'] >= pd.to_datetime(start).tz_localize('UTC')) &
        (df['period_of_report'] <= pd.to_datetime(end).tz_localize('UTC'))
    )

    acquired = (
        df[(df["acquired_disposed"] == "A") & mask]
        .groupby("issuer_ticker")["total_value"]
        .sum()
        .sort_values(ascending=False)
    )

    disposed = (
        df[(df["acquired_disposed"] == "D") & mask]
        .groupby("issuer_ticker")["total_value"]
        .sum()
        .sort_values(ascending=False)
    )

    return acquired, disposed

def companies_bs_in_period_by_reporter(df: pd.DataFrame, start, end, ticker=None):
    mask = (
        (df['period_of_report'] >= pd.to_datetime(start).tz_localize('UTC')) &
        (df['period_of_report'] <= pd.to_datetime(end).tz_localize('UTC'))
    )
    # safe pattern
    # Convert everything to UTC instead of localizing blindly
    # start = pd.to_datetime(start, utc=True)
    # end   = pd.to_datetime(end, utc=True)
    if ticker is not None:
        mask &= df['issuer_ticker'] == ticker

    acquired = (
        df[(df["acquired_disposed"] == "A") & mask]
        .groupby(["reporter", "issuer_ticker"])["total_value"]
        .sum()
        .sort_values(ascending=False)
    )

    disposed = (
        df[(df["acquired_disposed"] == "D") & mask]
        .groupby(["reporter", "issuer_ticker"])["total_value"]
        .sum()
        .sort_values(ascending=False)
    )

    return acquired, disposed

def distribution_by_codes(df: pd.DataFrame):
    """Distribution of transaction codes by acquired/disposed."""
    return (
        df.groupby(["acquired_disposed", "code"])["total_value"]
        .sum()
        .sort_values(ascending=False)
    )

def sector_stats_by_year(df: pd.DataFrame):
    sector_trades = df[['sector', 'total_value', 'period_of_report', 'acquired_disposed']].copy()
    sector_trades = sector_trades[sector_trades["sector"] != ""]
    sector_trades['period_of_report'] = pd.to_datetime(sector_trades['period_of_report'])
    sector_trades = sector_trades.set_index('period_of_report')
    sector_trades.groupby([pd.Grouper(freq='Y'), "acquired_disposed", "sector"]).head(5)

    sector_trades[sector_trades["acquired_disposed"]=="D"] \
                .groupby([pd.Grouper(freq='Y'), "acquired_disposed", "sector"])['total_value'] \
                .sum() \
                .unstack()
    
    sector_trades = sector_trades[sector_trades["acquired_disposed"]=="A"]
    return sector_trades.groupby([pd.Grouper(freq='Y'), "acquired_disposed", "sector"])['total_value'].sum()