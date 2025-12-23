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
KRAKEN_API = "https://api.kraken.com/0/public/OHLC"

SYMBOL_MAP = {"ETH/USD": "ETH-USD"}

# -------------------------
# Gap detection SQL (SAFE)
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
  WHERE exchange = 'kraken'
    AND symbol = 'ETH-USD'
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
# Kraken REST fetch
# -------------------------
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

# -------------------------
# Dagster Asset
# -------------------------
@asset(
    name="bronze_ohlcv_kraken_rest_1m",
    partitions_def=hourly_partitions,
    deps=["bronze_ohlcv_native"],
    description="Kraken REST 1m gap backfill (missing minutes only)",
)
def bronze_ohlcv_kraken_rest_1m(context: AssetExecutionContext) -> None:
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

    context.log.info(f"Missing Kraken minutes BEFORE REST: {missing_before}")

    if missing_before == 0:
        context.log.info("No gaps detected — skipping REST backfill")
        return

    # -------------------------
    # Fetch REST data
    # -------------------------
    rows = fetch_kraken_ohlc("ETH/USD", int(window_start.timestamp()))

    if not rows:
        raise RuntimeError("Kraken REST returned 0 rows during gap backfill")

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
            f"🚨 Kraken REST gap fill FAILED\n"
            f"Window: {window_start} → {window_end}\n"
            f"Missing before: {missing_before}\n"
            f"Missing after: {missing_after}"
        )
        send_slack_message(text=msg)
        raise Exception(msg)

    send_slack_message(
        text=(
            f"✅ Kraken REST gap fill successful\n"
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
            "source": "kraken_rest",
        }
    )