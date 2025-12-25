# kline_pipeline/jobs.py

from dagster import define_asset_job, AssetSelection
from .partitions import hourly_partitions


# --------------------------------
# Full hourly pipeline (v2-style)
# --------------------------------
hourly_assets_job = define_asset_job(
    name="hourly_assets_job",
    partitions_def=hourly_partitions,
)


# --------------------------------
# Bronze-only hourly job
# --------------------------------
bronze_ohlcv_hourly_job = define_asset_job(
    name="bronze_ohlcv_hourly_job",
    selection=AssetSelection.keys(
        "bronze_ohlcv_kraken_1m",
        "bronze_ohlcv_binance_1m",
    ),
    partitions_def=hourly_partitions,
)