# asset_keys.py
from dagster import AssetKey

def bronze_rest_key(exchange: str, rest_pair: str) -> AssetKey:
    normalized = rest_pair.lower().replace("/", "").replace("-", "")
    if rest_pair == '1000REKTUSDT':
        return AssetKey(f"bronze_ohlcv_{exchange}_rektusdt_rest_1m_v2")
    return AssetKey(f"bronze_ohlcv_{exchange}_{normalized}_rest_1m_v2")