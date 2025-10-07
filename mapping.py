import requests
import pandas as pd

MAPPING_ENDPOINT = "https://api.sec-api.io/mapping/exchange/{exchange}"

def load_exchange_mapping(exchanges=( "nasdaq", "nyse" )) -> pd.DataFrame:
    """
    Fetch company metadata (ticker, sector, industry, exchange) from SEC-API Mapping endpoints.
    Returns a DataFrame with columns: issuerTicker, cik, exchange, sector, industry, category, name
    """
    frames = []
    for ex in exchanges:
        url = MAPPING_ENDPOINT.format(exchange=ex)
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data)
        if df.empty:
            continue
        df.rename(columns={"ticker": "issuerTicker"}, inplace=True)
        keep = ["issuerTicker","cik","exchange","sector","industry","category","name"]
        frames.append(df[keep])
    if not frames:
        return pd.DataFrame(columns=["issuerTicker","cik","exchange","sector","industry","category","name"])
    out = pd.concat(frames).drop_duplicates("issuerTicker")
    return out
