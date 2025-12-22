from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext
from google.cloud import bigquery

from .partitions import hourly_partitions

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

MERGE_SQL = f"""
MERGE `kline-pipeline.market_data.fact_ohlcv` AS T
USING (

  WITH ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY exchange, symbol, interval_seconds, interval_start
        ORDER BY
          -- Prefer final candles if present
          ingestion_ts DESC
      ) AS rn
    FROM `kline-pipeline.market_data.bronze_ohlcv_native`
    WHERE exchange = 'kraken'
      AND symbol = 'ETH-USD'
      AND interval_seconds = 60
      AND interval_start >= @window_start
      AND interval_start <  @window_end
  )

  SELECT
    exchange,
    symbol,
    'USD' AS quote_asset,
    interval_seconds,
    interval_start,
    interval_end,
    open,
    high,
    low,
    close,
    volume,
    NULL AS quote_volume,
    trade_count,
    vwap,
    source,
    ingestion_ts
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
    interval_end  = S.interval_end,
    open          = S.open,
    high          = S.high,
    low           = S.low,
    close         = S.close,
    volume        = S.volume,
    quote_volume  = S.quote_volume,
    trade_count   = S.trade_count,
    vwap          = S.vwap,
    source        = S.source,
    ingestion_ts  = S.ingestion_ts

WHEN NOT MATCHED THEN
  INSERT (
    exchange,
    symbol,
    quote_asset,
    interval_seconds,
    interval_start,
    interval_end,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    trade_count,
    vwap,
    source,
    ingestion_ts
  )
  VALUES (
    S.exchange,
    S.symbol,
    S.quote_asset,
    S.interval_seconds,
    S.interval_start,
    S.interval_end,
    S.open,
    S.high,
    S.low,
    S.close,
    S.volume,
    S.quote_volume,
    S.trade_count,
    S.vwap,
    S.source,
    S.ingestion_ts
  );
"""

SILVER_COUNT_SQL = f"""
SELECT COUNT(*) AS row_count
FROM `{PROJECT_ID}.{DATASET}.fact_ohlcv`
WHERE exchange = 'kraken'
  AND symbol = 'ETH-USD'
  AND interval_seconds = 60
  AND interval_start >= @window_start
  AND interval_start <  @window_end
"""

@asset(
    name="fact_ohlcv_kraken_eth_1m",
    partitions_def=hourly_partitions,
    deps=["bronze_ohlcv_native"],
    description="Silver OHLCV from native bronze",
)
def fact_ohlcv_kraken_eth_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    window_start = datetime.fromisoformat(context.partition_key)
    window_end = window_start + timedelta(hours=1)

    context.log.info(
        f"Silver merge Kraken ETH-USD | {window_start} → {window_end}"
    )

    merge_job = client.query(
        MERGE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )
    merge_job.result()

    # ✅ VALIDATION STEP
    count_job = client.query(
        SILVER_COUNT_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )

    row_count = list(count_job.result())[0]["row_count"]

    if row_count == 0:
        raise Exception(
            f"Silver validation failed: no rows for {window_start} → {window_end}"
        )

    context.add_output_metadata(
        {
            "window_start": str(window_start),
            "window_end": str(window_end),
            "rows_present": row_count,
            "job_id": merge_job.job_id,
        }
    )