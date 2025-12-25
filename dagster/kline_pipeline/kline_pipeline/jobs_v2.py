# kline_pipeline/jobs_v2.py

from dagster import define_asset_job, AssetSelection
from .partitions import hourly_partitions


v2_hourly_assets_job = define_asset_job(
    name="v2_hourly_assets_job",
    partitions_def=hourly_partitions,
    selection=AssetSelection.keys(
        # ---- EXT → BRONZE (native bronze v2)
        "bronze_ohlcv_native_binance_1m_v2",
        "bronze_ohlcv_native_kraken_1m_v2",

        # ---- SILVER
        "fact_ohlcv_eth_1m",
    ),
)