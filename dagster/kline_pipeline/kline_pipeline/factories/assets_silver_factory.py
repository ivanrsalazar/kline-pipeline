from dagster import asset, AssetKey
from datetime import datetime, timedelta, timezone
import psycopg2
import os

from ..slack import send_slack_message
from ..partitions import hourly_partitions
from ..asset_keys import bronze_rest_key
from ..asset_config import EXCHANGES

PG_DSN = os.environ["PG_DSN"]
ROLLING_LOOKBACK_MINUTES = 180  # covers backfills + current hour


def make_silver_asset(symbol: str, exchanges: list[str]):
    asset_name = f"fact_ohlcv_{symbol.lower().replace('-', '')}_1m_v2"

    deps: list[AssetKey] = []
    for ex in exchanges:
        rest_pair = EXCHANGES[ex]["symbols"][symbol]["rest_pair"]
        deps.append(bronze_rest_key(ex, rest_pair))

    @asset(
        name=asset_name,
        partitions_def=hourly_partitions,
        deps=deps,
        description=f"Silver OHLCV {symbol} 1m",
        group_name="silver_v2",
    )
    def _asset(context):
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        # ---------------------------------------------
        # Define merge window
        # ---------------------------------------------
        partition_start = datetime.fromisoformat(
            context.partition_key
        ).replace(tzinfo=timezone.utc)

        window_end = partition_start
        window_start = window_end - timedelta(minutes=ROLLING_LOOKBACK_MINUTES)

        # ---------------------------------------------
        # Mark run start (for delta detection)
        # ---------------------------------------------
        run_started_at = datetime.now(tz=timezone.utc)

        # ---------------------------------------------
        # MERGE bronze → silver
        # ---------------------------------------------
        cur.execute(
            """
            MERGE INTO silver.fact_ohlcv tgt
            USING (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                exchange,
                                symbol,
                                interval_seconds,
                                interval_start
                            ORDER BY ingestion_ts DESC
                        ) AS rn
                    FROM bronze.bronze_ohlcv_native
                    WHERE interval_start >= %s
                      AND interval_start < %s
                ) dedup
                WHERE rn = 1
            ) src
            ON (
                tgt.exchange = src.exchange
                AND tgt.symbol = src.symbol
                AND tgt.interval_seconds = src.interval_seconds
                AND tgt.interval_start = src.interval_start
            )
            WHEN MATCHED AND src.ingestion_ts > tgt.ingestion_ts THEN
                UPDATE SET
                    open = src.open,
                    high = src.high,
                    low = src.low,
                    close = src.close,
                    volume = src.volume,
                    vwap = src.vwap,
                    trade_count = src.trade_count,
                    source = src.source,
                    ingestion_ts = src.ingestion_ts
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
                    src.exchange,
                    src.symbol,
                    'USD',
                    src.interval_seconds,
                    src.interval_start,
                    src.interval_end,
                    src.open,
                    src.high,
                    src.low,
                    src.close,
                    src.volume,
                    src.volume * src.close,
                    src.trade_count,
                    src.vwap,
                    src.source,
                    src.ingestion_ts
                );
            """,
            (window_start, window_end),
        )

        conn.commit()

        # ---------------------------------------------
        # Count rows written *this run*, per hour/exchange
        # ---------------------------------------------
        cur.execute(
            """
            SELECT
                exchange,
                date_trunc('hour', interval_start) AS hour,
                COUNT(*) AS rows_written
            FROM silver.fact_ohlcv
            WHERE ingestion_ts >= %s
              AND interval_start >= %s
              AND interval_start < %s
            GROUP BY exchange, hour
            ORDER BY hour, exchange
            """,
            (run_started_at, window_start, window_end),
        )

        written = cur.fetchall()

        # ---------------------------------------------
        # Gap detection per hour
        # ---------------------------------------------
        cur.execute(
            """
            SELECT
                exchange,
                date_trunc('hour', interval_start) AS hour,
                COUNT(DISTINCT interval_start) AS candles
            FROM silver.fact_ohlcv
            WHERE interval_start >= %s
              AND interval_start < %s
            GROUP BY exchange, hour
            ORDER BY hour, exchange
            """,
            (window_start, window_end),
        )

        completeness = {}
        for ex, hr, cnt in cur.fetchall():
            hour_key = hr.isoformat()
            completeness.setdefault(hour_key, {})[ex] = cnt

        # ---------------------------------------------
        # Build Slack message
        # ---------------------------------------------
        lines = [
            f"✅ Silver {symbol} 1m (this run)",
            "",
        ]

        # Hours touched in THIS run
        hours = sorted({h for _, h, _ in written})

        if not hours:
            lines.append("No rows written in this run.")
        else:
            for hour in hours:
                hour_key = hour.isoformat()

                lines.append(
                    f"{hour:%H:%M} → {(hour + timedelta(hours=1)):%H:%M}"
                )

                for ex in exchanges:
                    # rows written THIS run
                    delta = next(
                        (r for e, h, r in written if e == ex and h == hour),
                        0,
                    )

                    # total rows present after merge
                    total = completeness.get(hour_key, {}).get(ex, 0)

                    status = "✅" if total == 60 else "⚠️"

                    lines.append(
                        f"  {ex}: +{delta} ({total}/60) {status}"
                    )

                lines.append("")

        send_slack_message(text="\n".join(lines))

        # ---------------------------------------------
        # Metadata
        # ---------------------------------------------
        context.add_output_metadata(
            {
                "window": f"{window_start.isoformat()} → {window_end.isoformat()}",
                "rows_written": [
                    {
                        "exchange": ex,
                        "hour": hr.isoformat(),
                        "rows": rows,
                    }
                    for ex, hr, rows in written
                ],
                "completeness": completeness,
            }
        )

        cur.close()
        conn.close()

    return _asset