from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from google.cloud import bigquery

from .partitions import hourly_partitions

# --------------------
# Configuration
# --------------------
PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

# --------------------
# SQL: EXT → BRONZE NATIVE
# --------------------
INSERT_BRONZE_SQL = f"""
INSERT INTO `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native` (
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
  vwap,
  trade_count,
  source,
  ingestion_ts
)
SELECT
  exchange,
  symbol,
  interval_minutes * 60 AS interval_seconds,
  interval_start,
  interval_end,

  CAST(JSON_VALUE(payload, '$.data[0].open')   AS FLOAT64) AS open,
  CAST(JSON_VALUE(payload, '$.data[0].high')   AS FLOAT64) AS high,
  CAST(JSON_VALUE(payload, '$.data[0].low')    AS FLOAT64) AS low,
  CAST(JSON_VALUE(payload, '$.data[0].close')  AS FLOAT64) AS close,
  CAST(JSON_VALUE(payload, '$.data[0].volume') AS FLOAT64) AS volume,
  CAST(JSON_VALUE(payload, '$.data[0].vwap')   AS FLOAT64) AS vwap,
  CAST(JSON_VALUE(payload, '$.data[0].trades') AS INT64)   AS trade_count,

  'kraken_ws' AS source,
  CURRENT_TIMESTAMP() AS ingestion_ts
FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_ext`
WHERE exchange = 'kraken'
  AND symbol = 'ETH-USD'
  AND interval_minutes = 1
  AND interval_start >= @hour_start
  AND interval_start <  @hour_end
"""

# --------------------
# Dagster Asset
# --------------------
@asset(
    name="bronze_ohlcv_native",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(
        max_retries=5,
        delay=300,  # 5 minutes
    ),
    description="Load Kraken ETH-USD 1m OHLCV from external table into native bronze",
)
def bronze_ohlcv_native(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)

    ready_time = hour_start + timedelta(minutes=5)

    if now < ready_time:
        raise Exception(
            f"Too early to process partition {context.partition_key}. "
            f"Current time {now.isoformat()} < ready time {ready_time.isoformat()}"
        )

    hour_end = hour_start.replace(minute=0, second=0) + timedelta(hours=1)

    context.log.info(
        f"Bronze ingest Kraken ETH-USD | {hour_start} → {hour_end}"
    )

    job = client.query(
        INSERT_BRONZE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "hour_start", "TIMESTAMP", hour_start
                ),
                bigquery.ScalarQueryParameter(
                    "hour_end", "TIMESTAMP", hour_end
                ),
            ]
        ),
    )

    job.result()

    if job.total_bytes_processed == 0:
        raise Exception("Zero bytes processed — retrying")

    context.add_output_metadata(
        {
            "hour": context.partition_key,
            "job_id": job.job_id,
            "bytes_processed": job.total_bytes_processed,
        }
    )