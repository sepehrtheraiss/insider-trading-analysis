import pandas as pd

def summarize_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Total dollar value by transaction code and acquired/disposed flag."""
    if df.empty:
        return df
    g = (df
         .groupby(["code","acquiredDisposed"], dropna=False)["totalValue"]
         .sum()
         .reset_index()
         .sort_values("totalValue", ascending=False)
    )
    return g

def sector_year_pivot(df: pd.DataFrame) -> pd.DataFrame:
    dfi = df.copy()
    dfi["year"] = dfi["transactionDate"].dt.year
    pv = (dfi.pivot_table(index="sector", columns="year", values="totalValue", aggfunc="sum"))
    return pv

def top_reporters(df: pd.DataFrame, n=20) -> pd.DataFrame:
    dfi = df.copy()
    g = (dfi.groupby(["reporter","issuerTicker"], dropna=False)["totalValue"]
             .sum()
             .reset_index()
             .sort_values("totalValue", ascending=False)
        )
    return g.head(n)
