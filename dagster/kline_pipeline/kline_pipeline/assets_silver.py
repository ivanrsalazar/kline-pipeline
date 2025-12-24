from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext
from google.cloud import bigquery
from .slack import send_slack_message
from .partitions import hourly_partitions

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

ROLLING_LOOKBACK_MINUTES = 180  # 👈 THIS SOLVES YOUR ISSUE

MERGE_SQL = f"""
MERGE `{PROJECT_ID}.{DATASET}.fact_ohlcv` AS T
USING (
  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY exchange, symbol, interval_seconds, interval_start
        ORDER BY ingestion_ts DESC
      ) AS rn
    FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
    WHERE interval_seconds = 60
      AND interval_start >= @merge_start
      AND interval_start <  @merge_end
      AND exchange IN ('kraken', 'binance')
  )
  SELECT *
  FROM ranked
  WHERE rn = 1
) AS S
ON
  T.exchange = S.exchange
  AND T.symbol = S.symbol
  AND T.interval_seconds = S.interval_seconds
  AND T.interval_start = S.interval_start

WHEN MATCHED THEN
  UPDATE SET
    interval_end = S.interval_end,
    open = S.open,
    high = S.high,
    low = S.low,
    close = S.close,
    volume = S.volume,
    trade_count = S.trade_count,
    vwap = S.vwap,
    source = S.source,
    ingestion_ts = S.ingestion_ts

WHEN NOT MATCHED THEN
  INSERT (
    exchange,
    symbol,
    interval_seconds,
    interval_start,
    interval_end,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap,
    source,
    ingestion_ts
  )
  VALUES (
    S.exchange,
    S.symbol,
    S.interval_seconds,
    S.interval_start,
    S.interval_end,
    S.open,
    S.high,
    S.low,
    S.close,
    S.volume,
    S.trade_count,
    S.vwap,
    S.source,
    S.ingestion_ts
  );
"""

COUNT_SQL = f"""
SELECT
  exchange,
  COUNT(*) AS row_count
FROM `{PROJECT_ID}.{DATASET}.fact_ohlcv`
WHERE interval_seconds = 60
  AND interval_start >= @window_start
  AND interval_start <  @window_end
GROUP BY exchange
"""

@asset(
    name="fact_ohlcv_eth_1m",
    partitions_def=hourly_partitions,
    deps=[
        "bronze_ohlcv_kraken_rest_1m",
        "bronze_ohlcv_binance_1m",
    ],
    description="Silver ETH 1m OHLCV (rolling reconciliation)",
)
def fact_ohlcv_eth_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    partition_start = datetime.fromisoformat(context.partition_key)
    partition_end = partition_start + timedelta(hours=1)

    merge_start = partition_start - timedelta(minutes=ROLLING_LOOKBACK_MINUTES)
    merge_end = partition_end

    # -------------------------
    # MERGE
    # -------------------------
    client.query(
        MERGE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("merge_start", "TIMESTAMP", merge_start),
                bigquery.ScalarQueryParameter("merge_end", "TIMESTAMP", merge_end),
            ]
        ),
    ).result()

    # -------------------------
    # VALIDATION (CURRENT HOUR ONLY)
    # -------------------------
    counts = {
        row["exchange"]: row["row_count"]
        for row in client.query(
            COUNT_SQL,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", partition_start),
                    bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", partition_end),
                ]
            ),
        ).result()
    }

    kraken_rows = counts.get("kraken", 0)
    binance_rows = counts.get("binance", 0)

    # -------------------------
    # SLACK LOGIC
    # -------------------------
    if binance_rows == 0:
        msg = (
            f"🚨 Silver FAILED\n"
            f"Binance missing for {partition_start} → {partition_end}"
        )
        send_slack_message(text=msg)
        raise Exception(msg)

    if kraken_rows == 60:
        send_slack_message(
            text=f"✅ Silver OK: 60 kraken + {binance_rows} binance rows ({partition_start})"
        )
    elif 0 < kraken_rows < 60:
        send_slack_message(
            text=(
                f"⚠️ Silver DEGRADED (Kraken)\n"
                f"{kraken_rows}/60 kraken rows\n"
                f"{binance_rows}/60 binance rows\n"
                f"REST backfill expected"
            )
        )
    else:
        send_slack_message(
            text=(
                f"⚠️ Silver WARNING\n"
                f"Kraken missing entirely for {partition_start}"
            )
        )

    context.add_output_metadata(
        {
            "partition_start": str(partition_start),
            "kraken_rows": kraken_rows,
            "binance_rows": binance_rows,
            "merge_window_minutes": ROLLING_LOOKBACK_MINUTES,
        }
    )