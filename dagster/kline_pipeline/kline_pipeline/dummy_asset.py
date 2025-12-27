from datetime import datetime, timezone, timedelta
from dagster import asset, AssetExecutionContext, RetryPolicy
from .partitions import hourly_partitions

@asset(
    name="dummy_asset",
    partitions_def=hourly_partitions,
    retry_policy=RetryPolicy(max_retries=5, delay=180),
    description="3 minute delay for ingestion",
)
def dummy_asset(context: AssetExecutionContext) -> None:
    hour_start = datetime.strptime(
        context.partition_key, "%Y-%m-%d-%H:%M"
    ).replace(tzinfo=timezone.utc)

    ready_time = hour_start + timedelta(minutes=3)
    now = datetime.now(timezone.utc)
    if now < ready_time:
        raise Exception(
            f"Not yet ingestion time"
        )
    
    context.add_output_metadata(
        {
            "hour": context.partition_key,
        }
    )
