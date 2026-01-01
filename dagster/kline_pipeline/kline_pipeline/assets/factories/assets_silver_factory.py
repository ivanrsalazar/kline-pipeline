from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import psycopg2
from dagster import AssetKey, asset

from ...resources.slack import send_slack_message
from ...schedules.partitions import hourly_partitions
from ..asset_config import EXCHANGES
from ..asset_keys import bronze_rest_key

PG_DSN = os.environ["PG_DSN"]


def warehouse_symbol(exchange: str, symbol: str) -> str:
    """
    Convert canonical symbol (BTC-USD) into a warehouse LIKE pattern.
    Notes:
      - Your warehouse symbols appear to be e.g. BTCUSD / BTCUSDT depending on exchange.
      - This is intentionally permissive, but still scoped by exchange in queries.
    """
    base = symbol.replace("-", "")
    return f"%{base}%"


DEDUP_SUBQUERY = """
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
"""


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

        # -------------------------------------------------
        # Rolling merge window (handles late REST backfill)
        # Partition key is an ISO timestamp string for the hour start.
        # -------------------------------------------------
        window_end = (
            datetime.fromisoformat(context.partition_key)
            .replace(tzinfo=timezone.utc)
            + timedelta(hours=1)
        )
        window_start = window_end - timedelta(hours=2)

        # =================================================
        # 1) UPDATE existing rows (newer ingestion_ts wins)
        # =================================================
        cur.execute(
            f"""
            UPDATE silver.fact_ohlcv tgt
            SET
                open = src.open,
                high = src.high,
                low = src.low,
                close = src.close,
                volume = src.volume,
                vwap = src.vwap,
                trade_count = src.trade_count,
                source = src.source,
                ingestion_ts = src.ingestion_ts
            FROM (
                {DEDUP_SUBQUERY}
            ) src
            WHERE tgt.exchange = src.exchange
              AND tgt.symbol = src.symbol
              AND tgt.interval_seconds = src.interval_seconds
              AND tgt.interval_start = src.interval_start
              AND src.ingestion_ts > tgt.ingestion_ts;
            """,
            (window_start, window_end),
        )

        # =================================================
        # 2) INSERT missing rows (partition-safe, idempotent)
        #
        # IMPORTANT:
        # - Do NOT use ON CONFLICT here unless the PARENT table
        #   has a matching UNIQUE/EXCLUSION constraint (partitioned-table gotcha).
        # - Rely on NOT EXISTS + your per-partition unique indexes.
        # =================================================
        cur.execute(
            f"""
            INSERT INTO silver.fact_ohlcv (
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
            SELECT
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
            FROM (
                {DEDUP_SUBQUERY}
            ) src
            WHERE NOT EXISTS (
                SELECT 1
                FROM silver.fact_ohlcv tgt
                WHERE tgt.exchange = src.exchange
                  AND tgt.symbol = src.symbol
                  AND tgt.interval_seconds = src.interval_seconds
                  AND tgt.interval_start = src.interval_start
            );
            """,
            (window_start, window_end),
        )

        conn.commit()

        # -------------------------------------------------
        # Slack: report warehouse state for THIS trading pair
        # -------------------------------------------------
        report_hour_start = (
            datetime.now(tz=timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
            - timedelta(hours=1)
        )
        report_hour_end = report_hour_start + timedelta(hours=1)

        results: list[tuple[str, str, int]] = []

        for ex in exchanges:
            wh_symbol = warehouse_symbol(ex, symbol)

            cur.execute(
                """
                SELECT COUNT(DISTINCT interval_start)
                FROM silver.fact_ohlcv
                WHERE exchange = %s
                  AND symbol LIKE %s
                  AND interval_start >= %s
                  AND interval_start < %s
                """,
                (ex, wh_symbol, report_hour_start, report_hour_end),
            )

            cnt = int(cur.fetchone()[0] or 0)
            results.append((ex, wh_symbol, cnt))

        # Only alert on problems
        lines: list[str] = []
        for ex, sym_like, cnt in results:
            if cnt != 60:
                lines.append(f"⚠️ {ex} {sym_like}: {cnt}/60")

        if lines:
            send_slack_message(text="\n".join(lines))

        context.add_output_metadata(
            {
                "merge_window_start": window_start.isoformat(),
                "merge_window_end": window_end.isoformat(),
                "reported_hour_start": report_hour_start.isoformat(),
                "reported_hour_end": report_hour_end.isoformat(),
                "exchanges_checked": exchanges,
            }
        )

        cur.close()
        conn.close()

    return _asset