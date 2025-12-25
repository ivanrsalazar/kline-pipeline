# factories/assets_bronze_rest_factory.py

from dagster import asset
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import requests

from ..slack import send_slack_message
from ..partitions import hourly_partitions
from .rest_clients import fetch_rest_rows

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"
NATIVE_TABLE = "bronze_ohlcv_native"

LOOKBACK_MINUTES = 120


# -------------------------
# Gap counter (INLINE)
# -------------------------
def count_missing_minutes(
    client,
    exchange: str,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
) -> int:
    sql = f"""
    WITH expected AS (
      SELECT ts AS interval_start
      FROM UNNEST(
        GENERATE_TIMESTAMP_ARRAY(
          @window_start,
          @window_end,
          INTERVAL 1 MINUTE
        )
      ) ts
    ),
    actual AS (
      SELECT DISTINCT interval_start
      FROM `{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}`
      WHERE exchange = @exchange
        AND symbol = @symbol
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

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("exchange", "STRING", exchange),
                bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )

    return list(job.result())[0]["missing_rows"]





# -------------------------
# Factory
# -------------------------
def make_bronze_rest_asset(exchange: str, symbol: str, rest_pair: str):
    asset_name = f"bronze_ohlcv_{exchange}_{symbol.lower().replace('-', '_')}_rest_1m"

    @asset(
        name=asset_name,
        partitions_def=hourly_partitions,
        deps=[f"bronze_ohlcv_{exchange}_{symbol.lower().replace('-', '_')}_1m"],
        description=f"{exchange.upper()} REST gap backfill {symbol} 1m",
    )
    def _asset(context):
        client = bigquery.Client(project=PROJECT_ID)

        window_end = datetime.fromisoformat(context.partition_key).replace(
            tzinfo=timezone.utc
        )
        window_start = window_end - timedelta(minutes=LOOKBACK_MINUTES)

        missing_before = count_missing_minutes(
            client, exchange, symbol, window_start, window_end
        )

        if missing_before == 0:
            context.log.info("No gaps detected")
            return

        rows = fetch_rest_rows(exchange, rest_pair, window_start, window_end)

        client.insert_rows_json(
            f"{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}",
            rows,
            row_ids=[None] * len(rows),
        )

        missing_after = count_missing_minutes(
            client, exchange, symbol, window_start, window_end
        )

        if missing_after > 0:
            send_slack_message(
                f"🚨 {exchange} REST gap fill FAILED {symbol} "
                f"before={missing_before} after={missing_after}"
            )
            raise Exception("REST gap fill incomplete")

        send_slack_message(
            f"✅ {exchange} REST gap fill OK {symbol} filled={missing_before}"
        )

    return _asset