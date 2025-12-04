import pandas as pd

def summarize_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Total dollar value by transaction code and acquired/disposed flag."""
    if df.empty:
        return df
    g = (df
         .groupby(["code","acquired_disposed"], dropna=False)["total_value"]
         .sum()
         .reset_index()
         .sort_values("total_value", ascending=False)
    )
    return g

def sector_year_pivot(df: pd.DataFrame) -> pd.DataFrame:
    dfi = df.copy()
    dfi["year"] = dfi["transaction_date"].dt.year
    pv = (dfi.pivot_table(index="sector", columns="year", values="total_value", aggfunc="sum"))
    return pv

def top_reporters(df: pd.DataFrame, n=20) -> pd.DataFrame:
    dfi = df.copy()
    g = (dfi.groupby(["reporter","issuer_ticker"], dropna=False)["total_value"]
             .sum()
             .reset_index()
             .sort_values("total_value", ascending=False)
        )
    return g.head(n)
