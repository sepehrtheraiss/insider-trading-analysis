import pandas as pd

def attach_mapping(df: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Merge a mapping DataFrame onto the main DataFrame based on issuerTicker.

    Performs a left join on the 'issuerTicker' column to enrich the input
    DataFrame with additional metadata (e.g., sector, company info, etc.)
    from the mapping table.

    Args:
        df (pd.DataFrame): Main dataset.
        mapping (pd.DataFrame): Mapping dataset containing 'issuerTicker'.

    Returns:
        pd.DataFrame: Enriched DataFrame with mapping columns added.
    """    
    if df.empty:
        return df
    out = df.merge(mapping, on="issuerTicker", how="left", suffixes=(None,None))
    return out

def filter_valid_exchanges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows traded on valid stock exchanges (NASDAQ or NYSE).

    Filters the input DataFrame to include only rows where the 'exchange'
    column value is one of ['NASDAQ', 'NYSE'].

    Args:
        df (pd.DataFrame): Input dataset containing an 'exchange' column.

    Returns:
        pd.DataFrame: Filtered DataFrame containing only valid exchanges.
    """    
    if df.empty:
        return df
    return df[df["exchange"].isin(["NASDAQ","NYSE"])].copy()

def remove_price_outliers(df: pd.DataFrame, price_col="pricePerShare", max_price=100000) -> pd.DataFrame:
    """
    Remove invalid or extreme price values from the dataset.

    Keeps only rows where:
        - The security is not delisted ('isDelisted' == False),
        - The price column is not null,
        - The price is greater than 0 and less than `max_price`.

    Args:
        df (pd.DataFrame): Input dataset containing price and delisting info.
        price_col (str): Column name containing the price per share.
        max_price (float): Upper limit for acceptable price values.

    Returns:
        pd.DataFrame: Cleaned DataFrame with outlier prices removed.
    """    
    if df.empty:
        return df
    out = df.copy()
    out = out[
        (out['isDelisted'] == False)
        & (out[price_col].notna())
        & (out[price_col] > 0)
        & (out[price_col] < max_price)
    ]    
    return out

def finalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform final cleanup and add standardized columns.

    Creates a new column 'code_simple' by filling NaN values in 'code' with
    empty strings. This ensures no missing values before exporting or further
    processing.

    Args:
        df (pd.DataFrame): Input dataset containing 'code' column.

    Returns:
        pd.DataFrame: Finalized DataFrame with standardized fields.
    """    
    out = df.copy()
    out["code_simple"] = out["code"].fillna("")
    return out
