from dagster import asset, AssetExecutionContext
from google.cloud import bigquery

# --------------------
# Configuration
# --------------------
PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

# --------------------
# MERGE SQL
# --------------------
MERGE_SQL = f"""
MERGE `{PROJECT_ID}.{DATASET}.fact_ohlcv` AS T
USING (

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
  FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
  WHERE exchange = 'kraken'
    AND symbol = 'ETH-USD'
    AND interval_seconds = 60
    AND interval_start >= @window_start
    AND interval_start <  @window_end

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
  )
"""

# --------------------
# Dagster Asset
# --------------------
@asset(
    name="fact_ohlcv_kraken_eth_1m",
    partitions_def=hourly_partitions,
    deps=["bronze_ohlcv_native"],
    description="Silver OHLCV fact table from native bronze (Kraken ETH-USD 1m)",
)
def fact_ohlcv_kraken_eth_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    # Partition window
    partition_dt = datetime.fromisoformat(context.partition_key)
    window_start = partition_dt
    window_end = partition_dt + timedelta(hours=1)

    context.log.info(
        f"Merging Kraken ETH-USD 1m | {window_start} → {window_end}"
    )

    job = client.query(
        MERGE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "window_start", "TIMESTAMP", window_start
                ),
                bigquery.ScalarQueryParameter(
                    "window_end", "TIMESTAMP", window_end
                ),
            ]
        ),
    )

    job.result()

    context.add_output_metadata(
        {
            "job_id": job.job_id,
            "bytes_processed": job.total_bytes_processed,
            "window_start": str(window_start),
            "window_end": str(window_end),
        }
    )
#-----------------------------------------------------------------------------------------------------------
#                                      BRONZE EXT -> BRONZE NATIVE
#-----------------------------------------------------------------------------------------------------------

INSERT_BRONZE_SQL = f"""
INSERT INTO `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
SELECT
  event_type,
  event_ts,
  interval_start,
  interval_end,
  is_final,
  payload,

  exchange,
  symbol,
  interval_minutes,

  year,
  month,
  day,
  hour,

  CURRENT_TIMESTAMP() AS ingestion_ts
FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_ext`
WHERE exchange = 'kraken'
  AND symbol = 'ETH-USD'
  AND interval_minutes = 1
  AND interval_start >= @hour_start
  AND interval_start <  @hour_end
"""


@asset(
    name="bronze_ohlcv_native",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(
        max_retries=5,
        delay=300,  # 5 minutes
    ),
)
def bronze_ohlcv_native(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    # Partition key format: YYYY-MM-DD-HH:MM
    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)
    hour_end = hour_start.replace(minute=59, second=59)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "hour_start", "TIMESTAMP", hour_start
            ),
            bigquery.ScalarQueryParameter(
                "hour_end", "TIMESTAMP", hour_end
            ),
        ]
    )

    job = client.query(INSERT_BRONZE_SQL, job_config=job_config)
    job.result()

    context.add_output_metadata(
        {
            "hour": context.partition_key,
            "job_id": job.job_id,
            "bytes_processed": job.total_bytes_processed,
        }
    )