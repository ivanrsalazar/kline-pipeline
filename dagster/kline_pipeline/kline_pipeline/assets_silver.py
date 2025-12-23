from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext
from google.cloud import bigquery
from .slack import send_slack_message
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
        ORDER BY ingestion_ts DESC
      ) AS rn
    FROM `kline-pipeline.market_data.bronze_ohlcv_native`
    WHERE exchange IN ('kraken', 'binance')
      AND interval_seconds = 60
      AND interval_start >= @window_start
      AND interval_start <  @window_end
  )

  SELECT
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
    trade_count   = S.trade_count,
    vwap          = S.vwap,
    source        = S.source,
    ingestion_ts  = S.ingestion_ts

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

SILVER_COUNT_SQL = f"""
SELECT
  COUNTIF(exchange = 'kraken')  AS kraken_rows,
  COUNTIF(exchange = 'binance') AS binance_rows
FROM `kline-pipeline.market_data.fact_ohlcv`
WHERE interval_seconds = 60
  AND interval_start >= @window_start
  AND interval_start <  @window_end
"""

@asset(
    name="fact_ohlcv_eth_1m",
    partitions_def=hourly_partitions,
    deps=[
        "bronze_ohlcv_kraken_rest_1m",          # kraken
        "bronze_ohlcv_binance_1m",      # binance
    ],
    description="Silver OHLCV ETH 1m from Kraken + Binance",
)
def fact_ohlcv_eth_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    window_start = datetime.fromisoformat(context.partition_key)
    window_end = window_start + timedelta(hours=1)

    context.log.info(
        f"Silver merge ETH 1m | {window_start} → {window_end}"
    )

    # -------------------------
    # Run MERGE
    # -------------------------
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

    # -------------------------
    # VALIDATION (per exchange)
    # ------------------------- 
    # after merge_job.result()

    count_job = client.query(
        SILVER_COUNT_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
                bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
            ]
        ),
    )

    rows = list(count_job.result())
    row = rows[0]

    kraken_rows = row["kraken_rows"]
    binance_rows = row["binance_rows"]

    if kraken_rows == 0 or binance_rows == 0:
        raise Exception(
            f"Silver validation failed for {window_start} → {window_end} | "
            f"kraken_rows={kraken_rows}, binance_rows={binance_rows}"
        )
    success_msg = f"success: {kraken_rows} kraken rows uploaded : {binance_rows} binance rows uploaded"
    send_slack_message(text=success_msg)

    context.add_output_metadata(
        {
            "window_start": str(window_start),
            "window_end": str(window_end),
            "kraken_rows": kraken_rows,
            "binance_rows": binance_rows,
            "job_id": merge_job.job_id,
        }
    )

    
             

    