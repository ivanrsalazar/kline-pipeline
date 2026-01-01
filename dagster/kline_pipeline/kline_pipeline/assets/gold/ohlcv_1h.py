from dagster import asset
from datetime import datetime, timedelta, timezone
from kline_pipeline.schedules.partitions import hourly_partitions
from ..asset_config import target_tokens
import psycopg2
import os

PG_DSN = os.environ["PG_DSN"]

deps = [f"fact_ohlcv_{token}usd_1m_v2" for token in target_tokens]

@asset(
    name="gold_ohlcv_1h",
    partitions_def=hourly_partitions,
    deps=deps,
    group_name="gold_v2",
    description="Hourly OHLCV candles derived from silver 1m data",
)
def gold_ohlcv_1h(context):
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    window_end = datetime.fromisoformat(context.partition_key).replace(
        tzinfo=timezone.utc
    ) + timedelta(hours=1)
    window_start = window_end - timedelta(hours=2)  # rolling safety window

    cur.execute(
        """
        INSERT INTO gold.ohlcv_1h (
            exchange,
            symbol,
            interval_start,
            interval_end,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            source,
            ingestion_ts
        )
        SELECT
            exchange,
            symbol,
            hour_start AS interval_start,
            hour_start + interval '1 hour' AS interval_end,

            (array_agg(open ORDER BY interval_start))[1] AS open,
            max(high) AS high,
            min(low) AS low,
            (array_agg(close ORDER BY interval_start DESC))[1] AS close,

            sum(volume),
            sum(quote_volume),
            sum(trade_count),
            'derived',
            max(ingestion_ts)
        FROM (
            SELECT *,
                   date_trunc('hour', interval_start) AS hour_start
            FROM silver.fact_ohlcv
            WHERE interval_start >= %s
              AND interval_start < %s
        ) s
        GROUP BY exchange, symbol, hour_start
        ON CONFLICT (exchange, symbol, interval_start)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trade_count = EXCLUDED.trade_count,
            ingestion_ts = EXCLUDED.ingestion_ts;
        """,
        (window_start, window_end),
    )

    conn.commit()

    context.add_output_metadata({
        "window": f"{window_start} → {window_end}",
        "interval": "1h",
    })

    cur.close()
    conn.close()