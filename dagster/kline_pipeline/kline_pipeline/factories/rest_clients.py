# rest_clients.py

from datetime import datetime, timedelta, timezone
from typing import List, Dict
import requests

LOOKBACK_MINUTES = 120

SYMBOL_MAP = {
    "ETH/USD": "ETHUSD",
}


def fetch_rest_rows(
    exchange: str,
    pair: str,
    window_start: datetime,
    window_end: datetime,
) -> List[Dict]:
    if exchange == "kraken":
        return fetch_kraken_ohlc(pair, window_start)
    elif exchange == "binance":
        return fetch_binance_ohlc(pair, window_start, window_end)
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")





# -------------------------
# Binance REST fetch
# -------------------------
BINANCE_API = "https://api.binance.us/api/v3/klines"


def fetch_binance_ohlc(pair: str, window_start, window_end):
    url = "https://api.binance.us/api/v3/klines"

    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)

    params = {
        "symbol": pair,
        "interval": "1m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()

    data = resp.json()

    rows = []
    for k in data:
        rows.append(
            (
                "binance",
                pair,
                60,
                datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                datetime.fromtimestamp(k[6] / 1000, tz=timezone.utc),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                float(k[7]),
                int(k[8]),
                "binance_rest",
                datetime.now(tz=timezone.utc),
            )
        )

    return rows

# -------------------------
# Kraken REST fetch
# -------------------------
KRAKEN_API = "https://api.kraken.com/0/public/OHLC"

def fetch_kraken_ohlc(
    pair: str,
    window_start: datetime
) -> list[tuple]:
    since = int(window_start.timestamp())

    params = {
        "pair": pair,
        "interval": 1,
        "since": since,
    }

    resp = requests.get(KRAKEN_API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    rows: list[tuple] = []
    ingestion_ts = datetime.now(tz=timezone.utc)

    for key, ticks in data["result"].items():
        if key == "last":
            continue

        for t in ticks:
            interval_start = datetime.fromtimestamp(t[0], tz=timezone.utc)
            interval_end = interval_start + timedelta(minutes=1)

            # Enforce window bounds explicitly
            if interval_start < window_start:
                continue

            rows.append(
                (
                    "kraken",                 # exchange
                    SYMBOL_MAP[pair],         # symbol (exchange-native)
                    60,                       # interval_seconds
                    interval_start,           # interval_start
                    interval_end,             # interval_end
                    float(t[1]),              # open
                    float(t[2]),              # high
                    float(t[3]),              # low
                    float(t[4]),              # close
                    float(t[6]),              # volume
                    float(t[5]),              # vwap
                    int(t[7]),                # trade_count
                    "kraken_rest",             # source
                    ingestion_ts,             # ingestion_ts
                )
            )

    return rows
