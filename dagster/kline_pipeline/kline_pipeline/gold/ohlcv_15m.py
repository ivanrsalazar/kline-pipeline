from dagster import asset
from datetime import datetime, timedelta, timezone
from ..partitions import hourly_partitions
import psycopg2
import os

PG_DSN = os.environ["PG_DSN"]

@asset(
    name="gold_ohlcv_15m",
    partitions_def=hourly_partitions,
    deps=["fact_ohlcv_ethusd_1m_v2",
          "fact_ohlcv_btcusd_1m_v2",
          "fact_ohlcv_solusd_1m_v2",], 
    group_name="gold_v2",
    description="15m OHLCV candles derived from silver 1m data",
)
def gold_ohlcv_15m(context):
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    window_end = datetime.fromisoformat(context.partition_key).replace(
        tzinfo=timezone.utc
    ) + timedelta(hours=1)
    window_start = window_end - timedelta(hours=2)  # rolling safety window

    cur.execute(
        """
        INSERT INTO gold.ohlcv_15m
        SELECT
            exchange,
            symbol,
            bucket_start AS interval_start,
            bucket_start + interval '15 minutes' AS interval_end,

            (array_agg(open  ORDER BY interval_start))[1]        AS open,
            max(high)                                            AS high,
            min(low)                                             AS low,
            (array_agg(close ORDER BY interval_start DESC))[1]   AS close,

            sum(volume)        AS volume,
            sum(quote_volume)  AS quote_volume,
            sum(trade_count)   AS trade_count,

            'derived'          AS source,
            max(ingestion_ts)  AS ingestion_ts
        FROM (
            SELECT
                exchange,
                symbol,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count,
                ingestion_ts,
                interval_start,
                date_trunc('hour', interval_start)
                + floor(extract(minute from interval_start) / 15) * interval '15 minutes'
                    AS bucket_start
            FROM silver.fact_ohlcv
        ) s
        GROUP BY exchange, symbol, bucket_start
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
        "interval": "15m",
    })

    cur.close()
    conn.close()