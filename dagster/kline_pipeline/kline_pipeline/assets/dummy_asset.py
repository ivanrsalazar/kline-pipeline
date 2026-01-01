from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from kline_pipeline.schedules.partitions import hourly_partitions
from kline_pipeline.resources.slack import send_slack_message

@asset(
    name="dummy_asset",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(max_retries=5, delay=180),
    group_name="dummy",
    description="3 minute delay for ingestion",
)
def dummy_asset(context: AssetExecutionContext) -> None:
    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)

    ready_time = hour_start + timedelta(minutes=3) + timedelta(hours=1)
    now = datetime.now(timezone.utc)
    if now < ready_time:
        raise Exception(
            f"Not yet ingestion time"
        )
    
    send_slack_message(text=f"{hour_start} -> {hour_start + timedelta(hours=1)}")
    
    context.add_output_metadata(
        {
            "hour": context.partition_key,
        }
    )
