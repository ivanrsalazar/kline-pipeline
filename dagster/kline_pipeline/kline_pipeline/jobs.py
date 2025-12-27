from dagster import define_asset_job, AssetSelection
from .partitions import hourly_partitions

hourly_assets_job = define_asset_job(
    name="hourly_assets_job",
    partitions_def=hourly_partitions,
    selection=AssetSelection.keys(
        # ---- LEGACY / STABLE ASSETS ONLY
        "bronze_ohlcv_binance_1m",
        "bronze_ohlcv_binance_rest_1m",
        "bronze_ohlcv_native",
        "fact_ohlcv_eth_1m",
        "bronze_ohlcv_kraken_rest_1m",
        "dummy_asset"
    ),
)