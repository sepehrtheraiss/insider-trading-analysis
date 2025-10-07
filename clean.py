import pandas as pd

def attach_mapping(df: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    m = mapping.rename(columns={"cik":"issuerCik"})
    out = df.merge(m, on="issuerTicker", how="left", suffixes=("",""))
    return out

def filter_valid_exchanges(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df["exchange"].isin(["NASDAQ","NYSE"])].copy()

def remove_price_outliers(df: pd.DataFrame, price_col="pricePerShare", max_price=100000) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out = out[(out[price_col].isna()) | ((out[price_col] > 0) & (out[price_col] < max_price))]
    return out

def finalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["code_simple"] = out["code"].fillna("")
    return out
