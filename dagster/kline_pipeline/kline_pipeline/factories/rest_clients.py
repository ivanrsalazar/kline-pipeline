# rest_clients.py

from datetime import datetime, timedelta, timezone
import requests

LOOKBACK_MINUTES = 120

def fetch_rest_rows(exchange: str, rest_pair: str, window_start: datetime) -> list[dict]:
    if exchange == "kraken":
        return fetch_kraken(rest_pair, window_start)
    elif exchange == "binance":
        return fetch_binance(rest_pair, window_start)
    else:
        raise ValueError(f"Unsupported exchange: {exchange}")


def fetch_kraken(pair: str, window_start: datetime) -> list[dict]:
    # placeholder – you already implemented this earlier
    raise NotImplementedError


# -------------------------
# Binance REST fetch
# -------------------------
BINANCE_API = "https://api.binance.us/api/v3/klines"


def fetch_binance_ohlc(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    params = {
        "symbol": symbol,
        "interval": "1m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }

    resp = requests.get(BINANCE_API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    now_ts = datetime.now(tz=timezone.utc).isoformat()

    for t in data:
        interval_start = datetime.fromtimestamp(t[0] / 1000, tz=timezone.utc)
        interval_end = datetime.fromtimestamp(t[6] / 1000, tz=timezone.utc)

        rows.append(
            {
                "exchange": "binance",
                "symbol": SYMBOL_MAP[symbol],
                "interval_seconds": 60,
                "interval_start": interval_start.isoformat(),
                "interval_end": interval_end.isoformat(),
                "open": float(t[1]),
                "high": float(t[2]),
                "low": float(t[3]),
                "close": float(t[4]),
                "volume": float(t[5]),
                "trade_count": int(t[8]),
                "vwap": None,  # Binance doesn’t give vwap directly
                "source": "binance_rest",
                "ingestion_ts": now_ts,
            }
        )

    return rows


# -------------------------
# Kraken REST fetch
# -------------------------
KRAKEN_API = "https://api.kraken.com/0/public/OHLC"

def fetch_kraken_ohlc(pair: str, since: int) -> list[dict]:
    params = {"pair": pair, "interval": 1, "since": since}
    resp = requests.get(KRAKEN_API, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    rows = []
    now_ts = datetime.now(tz=timezone.utc).isoformat()

    for key, ticks in data["result"].items():
        if key == "last":
            continue

        for t in ticks:
            interval_start = datetime.fromtimestamp(t[0], tz=timezone.utc)
            interval_end = interval_start + timedelta(minutes=1) - timedelta(milliseconds=1)

            rows.append(
                {
                    "exchange": "kraken",
                    "symbol": SYMBOL_MAP[pair],
                    "interval_seconds": 60,
                    "interval_start": interval_start.isoformat(),
                    "interval_end": interval_end.isoformat(),
                    "open": float(t[1]),
                    "high": float(t[2]),
                    "low": float(t[3]),
                    "close": float(t[4]),
                    "vwap": float(t[5]),
                    "volume": float(t[6]),
                    "trade_count": int(t[7]),
                    "source": "kraken_rest",
                    "ingestion_ts": now_ts,
                }
            )

    return rows
