from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from google.cloud import bigquery

from .partitions import hourly_partitions

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

INSERT_BRONZE_SQL = f"""
INSERT INTO `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
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

BRONZE_COUNT_SQL = f"""
SELECT COUNT(*) AS row_count
FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
WHERE exchange = 'kraken'
  AND symbol = 'ETH-USD'
  AND interval_seconds = 60
  AND interval_start >= @hour_start
  AND interval_start <  @hour_end
"""

@asset(
    name="bronze_ohlcv_native",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(max_retries=5, delay=300),
    deps=["dummy_asset"],
    description="Load Kraken ETH-USD 1m OHLCV from ext → native bronze",
)
def bronze_ohlcv_native(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)

    hour_end = hour_start + timedelta(hours=1)

    context.log.info(
        f"Bronze ingest Kraken ETH-USD | {hour_start} → {hour_end}"
    )

    job = client.query(
        INSERT_BRONZE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hour_start", "TIMESTAMP", hour_start),
                bigquery.ScalarQueryParameter("hour_end", "TIMESTAMP", hour_end),
            ]
        ),
    )
    job.result()

    # ✅ VALIDATION STEP
    count_job = client.query(
        BRONZE_COUNT_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hour_start", "TIMESTAMP", hour_start),
                bigquery.ScalarQueryParameter("hour_end", "TIMESTAMP", hour_end),
            ]
        ),
    )

    row_count = list(count_job.result())[0]["row_count"]

    if row_count == 0:
        raise Exception(
            f"Bronze validation failed: no rows for {hour_start} → {hour_end}"
        )

    context.add_output_metadata(
        {
            "hour": context.partition_key,
            "rows_written": row_count,
            "job_id": job.job_id,
        }
    )