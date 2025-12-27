from dagster import asset
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import execute_batch
import os

from ..slack import send_slack_message
from ..partitions import hourly_partitions
from .rest_clients import fetch_rest_rows

PG_DSN = os.environ["PG_DSN"]


# -------------------------------------------------
# Find WS-missing interval_start timestamps
# -------------------------------------------------
def get_ws_missing_minutes(
    cur,
    exchange: str,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
) -> set[datetime]:
    symbol_filter = symbol.replace("-", "")
    if exchange == "binance":
        symbol_filter += "T"

    ws_source = f"{exchange}_ws"

    cur.execute(
        """
        WITH expected AS (
            SELECT generate_series(%s, %s, interval '1 minute') AS interval_start
        ),
        actual AS (
            SELECT DISTINCT interval_start
            FROM bronze.bronze_ohlcv_native
            WHERE exchange = %s
              AND symbol = %s
              AND interval_seconds = 60
              AND source = %s
              AND interval_start >= %s
              AND interval_start < %s
        )
        SELECT e.interval_start
        FROM expected e
        LEFT JOIN actual a USING (interval_start)
        WHERE a.interval_start IS NULL
        """,
        (
            window_start,
            window_end - timedelta(minutes=1),
            exchange,
            symbol_filter,
            ws_source,
            window_start,
            window_end,
        ),
    )

    return {row[0] for row in cur.fetchall()}


# -------------------------------------------------
# Factory
# -------------------------------------------------
def make_bronze_rest_asset(exchange: str, symbol: str, rest_pair: str):
    asset_name = f"bronze_ohlcv_{exchange}_{rest_pair.lower().replace('/', '')}_rest_1m_v2"

    @asset(
        name=asset_name,
        partitions_def=hourly_partitions,
        deps=[f"bronze_ohlcv_native_{exchange}_1m_v2"],
        description=f"{exchange.upper()} REST WS-gap fill {symbol} 1m",
        group_name="bronze_rest_v2",
    )
    def _asset(context):
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        # ---------------------------------------------
        # Backfill previous completed hour ONLY
        # ---------------------------------------------
        window_end = datetime.fromisoformat(context.partition_key).replace(
            tzinfo=timezone.utc
        ) + timedelta(hours=1)
        window_start = window_end - timedelta(hours=2)

        ws_missing = get_ws_missing_minutes(
            cur, exchange, symbol, window_start, window_end
        )

        if not ws_missing:
            context.log.info("No WS gaps detected")
            cur.close()
            conn.close()
            return

        context.log.info(
            f"Detected {len(ws_missing)} WS gaps — fetching REST"
        )

        # ---------------------------------------------
        # Fetch REST rows for the hour
        # ---------------------------------------------
        rest_rows = fetch_rest_rows(
            exchange, rest_pair, window_start, window_end
        )

        # ---------------------------------------------
        # Insert ONLY rows that fill WS gaps
        # ---------------------------------------------
        rows = [
            row
            for row in rest_rows
            if row[3] in ws_missing  # interval_start
        ]

        if not rows:
            context.log.warning(
                "REST returned no rows for WS-missing minutes "
                "(likely zero-trade intervals)"
            )
            cur.close()
            conn.close()
            return

        sql = """
        INSERT INTO bronze.bronze_ohlcv_native (
            exchange, symbol, interval_seconds,
            interval_start, interval_end,
            open, high, low, close,
            volume, vwap, trade_count,
            source, ingestion_ts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        """

        execute_batch(cur, sql, rows, page_size=500)
        conn.commit()

        # ---------------------------------------------
        # Validate: REST rows exist for attempted minutes
        # ---------------------------------------------
        cur.execute(
            """
            SELECT COUNT(DISTINCT interval_start)
            FROM bronze.bronze_ohlcv_native
            WHERE exchange = %s
              AND symbol = %s
              AND interval_start = ANY(%s)
            """,
            (
                exchange,
                symbol.replace("-", "") + ("T" if exchange == "binance" else ""),
                list(ws_missing),
            ),
        )

        filled = cur.fetchone()[0]

        if filled == 0:
            send_slack_message(
                f"⚠️ {exchange} REST returned data but none inserted {symbol}"
            )
        else:
            send_slack_message(
                f"✅ {exchange} REST WS-gap fill OK {symbol} filled={filled}"
            )

        context.add_output_metadata(
            {
                "ws_missing": len(ws_missing),
                "filled_minutes": filled,
                "window": f"{window_start.isoformat()} → {window_end.isoformat()}",
            }
        )

        cur.close()
        conn.close()

    return _asset