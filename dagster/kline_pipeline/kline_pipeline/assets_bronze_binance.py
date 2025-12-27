from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from google.cloud import bigquery

from .partitions import hourly_partitions

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"

# --------------------
# SQL: EXT → BRONZE (BINANCE)
# --------------------
INSERT_BINANCE_BRONZE_SQL = f"""
INSERT INTO `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
SELECT
  exchange,
  symbol,
  interval_minutes * 60 AS interval_seconds,
  interval_start,
  interval_end,

  CAST(JSON_VALUE(payload, '$.k.o') AS FLOAT64) AS open,
  CAST(JSON_VALUE(payload, '$.k.h') AS FLOAT64) AS high,
  CAST(JSON_VALUE(payload, '$.k.l') AS FLOAT64) AS low,
  CAST(JSON_VALUE(payload, '$.k.c') AS FLOAT64) AS close,
  CAST(JSON_VALUE(payload, '$.k.v') AS FLOAT64) AS volume,
  CAST(JSON_VALUE(payload, '$.k.q') AS FLOAT64) AS vwap,
  CAST(JSON_VALUE(payload, '$.k.n') AS INT64)   AS trade_count,

  'binance_ws' AS source,
  CURRENT_TIMESTAMP() AS ingestion_ts

FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_ext`
WHERE exchange = 'binance'
  AND symbol = 'ETHUSDT'
  AND interval_minutes = 1
  AND interval_start >= @hour_start
  AND interval_start <  @hour_end
"""

# --------------------
# VALIDATION QUERY
# --------------------
BINANCE_ROWCOUNT_SQL = f"""
SELECT COUNT(*) AS row_count
FROM `{PROJECT_ID}.{DATASET}.bronze_ohlcv_native`
WHERE exchange = 'binance'
  AND symbol = 'ETHUSDT'
  AND interval_seconds = 60
  AND interval_start >= @hour_start
  AND interval_start <  @hour_end
"""

# --------------------
# DAGSTER ASSET
# --------------------
@asset(
    name="bronze_ohlcv_binance_1m",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(max_retries=5, delay=300),
    deps=["dummy_asset"],
    description="Load Binance ETHUSDT 1m OHLCV into native bronze (validated)",
)
def bronze_ohlcv_binance_1m(context: AssetExecutionContext) -> None:
    client = bigquery.Client(project=PROJECT_ID)

    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)

    hour_end = hour_start + timedelta(hours=1)

    context.log.info(
        f"Bronze ingest Binance ETHUSDT | {hour_start} → {hour_end}"
    )

    # ---- Insert
    insert_job = client.query(
        INSERT_BINANCE_BRONZE_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hour_start", "TIMESTAMP", hour_start),
                bigquery.ScalarQueryParameter("hour_end", "TIMESTAMP", hour_end),
            ]
        ),
    )
    insert_job.result()

    # ---- Validate
    count_job = client.query(
        BINANCE_ROWCOUNT_SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hour_start", "TIMESTAMP", hour_start),
                bigquery.ScalarQueryParameter("hour_end", "TIMESTAMP", hour_end),
            ]
        ),
    )
    row_count = list(count_job.result())[0]["row_count"]

    context.log.info(f"Binance bronze rows ingested: {row_count}")

    if row_count == 0:
        raise Exception(
            f"Zero Binance rows ingested for {hour_start} → {hour_end}"
        )

    context.add_output_metadata(
        {
            "hour": context.partition_key,
            "rows_ingested": row_count,
            "insert_job_id": insert_job.job_id,
        }
    )