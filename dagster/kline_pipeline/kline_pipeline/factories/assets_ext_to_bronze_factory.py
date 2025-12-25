from dagster import asset, AssetExecutionContext
from google.cloud import bigquery

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"


def make_ext_to_bronze_asset(
    *,
    exchange: str,
    ext_table: str,
    native_table: str,
):
    """
    EXT → native bronze ingestion

    Assumes:
    - ext_table has JSON payloads + partition columns
    - native_table is typed and stable
    """

    INSERT_SQL = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET}.{native_table}` (
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

        CONCAT(exchange, '_ws') AS source,
        CURRENT_TIMESTAMP() AS ingestion_ts

    FROM `{PROJECT_ID}.{DATASET}.{ext_table}`
    WHERE exchange = '{exchange}';
    """

    @asset(
        name=f"{native_table}_{exchange}_1m_v2",
        description=f"EXT → native bronze OHLCV ({exchange})",
    )
    def _asset(context: AssetExecutionContext) -> None:
        client = bigquery.Client(project=PROJECT_ID)

        context.log.info(
            f"Running EXT → bronze ingest | exchange={exchange}"
        )

        job = client.query(INSERT_SQL)
        job.result()

        context.add_output_metadata(
            {
                "job_id": job.job_id,
                "bytes_processed": job.total_bytes_processed,
            }
        )

    return _asset