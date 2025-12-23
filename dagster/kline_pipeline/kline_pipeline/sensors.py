from dagster import sensor, RunRequest, SkipReason
from datetime import datetime, timedelta, timezone

from .slack import send_slack_message


@sensor(minimum_interval_seconds=300)  # every 5 minutes
def retry_stale_failed_partitions(context):
    now = datetime.now(timezone.utc)

    asset_records = context.instance.get_asset_records()

    stale_partitions = []

    for record in asset_records:
        asset_key = record.asset_entry.asset_key

        # Only care about this asset
        if asset_key.path[-1] != "fact_ohlcv_eth_1m":
            continue

        last_materialization = record.asset_entry.last_materialization

        if not last_materialization:
            continue

        mat_ts = datetime.fromtimestamp(
            last_materialization.timestamp, tz=timezone.utc
        )

        # Older than 2 hours
        if now - mat_ts > timedelta(hours=2):
            stale_partitions.append(asset_key)

    if not stale_partitions:
        return SkipReason("No stale assets")

    send_slack_message(
        ":warning: *Dagster Alert*\n"
        f"{len(stale_partitions)} assets stale for >2 hours.\n"
        f"Asset: `{stale_partitions[0].to_string()}`"
    )

    for asset_key in stale_partitions:
        yield RunRequest(
            asset_selection=[asset_key],
            run_key=f"retry-{asset_key.to_string()}",
        )