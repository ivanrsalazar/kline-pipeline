# assets_silver_factory.py

from dagster import asset
from datetime import datetime, timedelta
from google.cloud import bigquery
from ..slack import send_slack_message
from ..partitions import hourly_partitions

PROJECT_ID = "kline-pipeline"
DATASET = "market_data"


def make_silver_asset(symbol: str, exchanges: list[str]):
    asset_name = f"fact_ohlcv_{symbol.lower().replace('-', '_')}_1m_v2"

    deps = [
        f"bronze_ohlcv_{ex}_{symbol.lower().replace('-', '_')}_rest_1m"
        for ex in exchanges
    ]

    @asset(
        name=asset_name,
        partitions_def=hourly_partitions,
        deps=deps,
        description=f"Silver OHLCV {symbol} 1m",
    )
    def _asset(context):
        client = bigquery.Client(project=PROJECT_ID)

        window_start = datetime.fromisoformat(context.partition_key)
        window_end = window_start + timedelta(hours=1)

        run_merge(client, symbol, window_start, window_end)

        counts = count_silver_rows(client, symbol, window_start, window_end)

        for ex in exchanges:
            if counts.get(ex, 0) < 60:
                send_slack_message(
                    f"⚠️ Silver incomplete {symbol} {ex}: "
                    f"{counts.get(ex, 0)}/60 rows"
                )

        context.add_output_metadata(counts)

    return _asset