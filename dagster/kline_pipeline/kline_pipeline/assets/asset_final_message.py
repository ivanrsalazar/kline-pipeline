from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from kline_pipeline.schedules.partitions import hourly_partitions
from kline_pipeline.resources.slack import send_slack_message
import psycopg2
import os
PG_DSN = os.environ["PG_DSN"]

@asset(
    name="final_message",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(max_retries=5, delay=180),
    deps=[
        "gold_ohlcv_1h",
        "gold_ohlcv_5m",
        "gold_ohlcv_15m",
        "gold_ohlcv_30m",
        ],
    group_name="final_message",
    description="Slack Message after ingestion",
)
def final_message(context: AssetExecutionContext) -> None:
    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)
    hour_end = hour_start + timedelta(hours=1)

    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(interval_start)
        FROM silver.fact_ohlcv
        WHERE interval_start >= %s
        AND interval_start < %s
        """,
        (hour_start, hour_end),
    )

    cnt = cur.fetchone()[0]

    if cnt == 30*60:
        send_slack_message(text=f"ok")
    
    context.add_output_metadata(
        {
            "hour": context.partition_key,
        }
    )
