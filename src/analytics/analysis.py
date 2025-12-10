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

def companies_bs_in_period(df: pd.DataFrame, year: int):
    """Top companies bought/sold in a given year."""
    mask = df["period_of_report"].dt.year == int(year)

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

def companies_bs_in_period_by_person(df: pd.DataFrame, year: int):
    mask = df["period_of_report"].dt.year == int(year)

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
    df2 = df.copy()
    return df2.groupby([pd.Grouper(freq="Y"), "acquired_disposed", "sector"])["total_value"].sum()
