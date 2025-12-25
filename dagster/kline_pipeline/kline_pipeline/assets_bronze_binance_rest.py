from datetime import datetime, timedelta, timezone
import requests

from dagster import asset, AssetExecutionContext
from google.cloud import bigquery

from .slack import send_slack_message
from .partitions import hourly_partitions

# -------------------------
# Config
# -------------------------
PROJECT_ID = "kline-pipeline"
DATASET = "market_data"
NATIVE_TABLE = "bronze_ohlcv_native"

LOOKBACK_MINUTES = 120
BINANCE_API = "https://api.binance.us/api/v3/klines"

SYMBOL_MAP = {
    "ETHUSDT": "ETH-USD",
}

# -------------------------
# Gap detection SQL
# -------------------------
GAP_CHECK_SQL = f"""
WITH bounds AS (
  SELECT
    @window_start AS window_start,
    @window_end   AS window_end
),
expected AS (
  SELECT ts AS interval_start
  FROM bounds,
  UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      window_start,
      window_end,
      INTERVAL 1 MINUTE
    )
  ) ts
),
actual AS (
  SELECT DISTINCT interval_start
  FROM `{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}`
  WHERE exchange = 'binance'
    AND symbol = 'ETHUSDT'
    AND interval_seconds = 60
    AND interval_start >= @window_start
    AND interval_start <= @window_end
)
SELECT COUNT(*) AS missing_rows
FROM expected e
LEFT JOIN actual a
USING (interval_start)
WHERE a.interval_start IS NULL
"""

# -------------------------
# Binance REST fetch
# -------------------------
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
# Dagster Asset
# -------------------------
@asset(
    name="bronze_ohlcv_binance_rest_1m",
    partitions_def=hourly_partitions,
    deps=["bronze_ohlcv_binance_1m"],
    description="Binance REST 1m gap backfill (missing minutes only)",
)
def bronze_ohlcv_binance_rest_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    window_end = datetime.fromisoformat(context.partition_key).replace(tzinfo=timezone.utc)
    window_start = window_end - timedelta(minutes=LOOKBACK_MINUTES)

    # -------------------------
    # Count missing BEFORE
    # -------------------------
    before_job = client.query(
        GAP_CHECK_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )
    missing_before = list(before_job.result())[0]["missing_rows"]

    context.log.info(f"Missing Binance minutes BEFORE REST: {missing_before}")

    if missing_before == 0:
        context.log.info("No Binance gaps detected — skipping REST backfill")
        return

    # -------------------------
    # Fetch REST data
    # -------------------------
    rows = fetch_binance_ohlc(
        symbol="ETHUSDT",
        start_ms=int(window_start.timestamp() * 1000),
        end_ms=int(window_end.timestamp() * 1000),
    )

    if not rows:
        raise RuntimeError("Binance REST returned 0 rows during gap backfill")

    errors = client.insert_rows_json(
        f"{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}",
        rows,
        row_ids=[None] * len(rows),
    )
    if errors:
        raise RuntimeError(errors)

    # -------------------------
    # Count missing AFTER
    # -------------------------
    after_job = client.query(
        GAP_CHECK_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )
    missing_after = list(after_job.result())[0]["missing_rows"]

    # -------------------------
    # Validate + Alert
    # -------------------------
    if missing_after > 0:
        msg = (
            f"🚨 Binance REST gap fill FAILED\n"
            f"Window: {window_start} → {window_end}\n"
            f"Missing before: {missing_before}\n"
            f"Missing after: {missing_after}"
        )
        send_slack_message(text=msg)
        raise Exception(msg)

    send_slack_message(
        text=(
            f"✅ Binance REST gap fill successful\n"
            f"Window: {window_start} → {window_end}\n"
            f"Missing before: {missing_before}\n"
            f"Missing after: 0"
        )
    )

    context.add_output_metadata(
        {
            "missing_before": missing_before,
            "missing_after": 0,
            "rows_inserted": len(rows),
            "lookback_minutes": LOOKBACK_MINUTES,
            "source": "binance_rest",
        }
    )