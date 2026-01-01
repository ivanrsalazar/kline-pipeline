from dagster import sensor, RunRequest, SkipReason
from datetime import datetime, timedelta, timezone
import psycopg2
import os

from kline_pipeline.resources.slack import send_slack_message

PG_DSN = os.environ["PG_DSN"]


@sensor(
    minimum_interval_seconds=21600,   # every 5 minutes
    job_name="v2_hourly_assets_job" # 👈 REQUIRED
)
def backfill_missing_ohlcv_partitions(context):
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=12)

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute(
        """
        WITH expected AS (
            SELECT
                exchange,
                symbol,
                date_trunc('hour', interval_start) AS hour,
                generate_series(
                    date_trunc('hour', interval_start),
                    date_trunc('hour', interval_start) + interval '59 minutes',
                    interval '1 minute'
                ) AS expected_minute
            FROM silver.fact_ohlcv
            WHERE interval_start >= %s
        ),
        actual AS (
            SELECT
                exchange,
                symbol,
                interval_start
            FROM silver.fact_ohlcv
            WHERE interval_start >= %s
        ),
        missing AS (
            SELECT
                e.exchange,
                e.symbol,
                e.hour,
                COUNT(*) AS missing_minutes
            FROM expected e
            LEFT JOIN actual a
              ON a.exchange = e.exchange
             AND a.symbol = e.symbol
             AND a.interval_start = e.expected_minute
            WHERE a.interval_start IS NULL
            GROUP BY e.exchange, e.symbol, e.hour
        )
        SELECT exchange, symbol, hour, missing_minutes
        FROM missing
        WHERE missing_minutes > 0
        ORDER BY hour DESC;
        """,
        (window_start, window_start),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return SkipReason("No missing OHLCV minutes detected")

    # Determine which partitions to rerun
    partitions_to_run = sorted(
        {
            hour.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d-%H:00")
            for _, _, hour, _ in rows
        }
    )

    # Slack summary
    lines = ["⚠️ *OHLCV gaps detected — triggering backfill*", ""]
    for exchange, symbol, hour, missing in rows:
        lines.append(
            f"- {exchange} {symbol} {hour:%Y-%m-%d %H}:00 → missing {missing}/60"
        )

    send_slack_message("\n".join(lines))

    # Trigger backfill runs
    for partition_key in partitions_to_run:
        yield RunRequest(
            partition_key=partition_key,
            run_key=f"gap-backfill-{partition_key}",
        )