# kline_pipeline/definitions.py
from dagster import Definitions

from .assets_bronze import bronze_ohlcv_native
from .assets_silver import fact_ohlcv_kraken_eth_1m
from .assets_bronze_binance import bronze_ohlcv_binance_1m

from .jobs import hourly_assets_job
from .sensors import retry_stale_failed_partitions
from .schedules import hourly_schedule

defs = Definitions(
    assets=[
        bronze_ohlcv_binance_1m,
        bronze_ohlcv_native,
        fact_ohlcv_kraken_eth_1m,
    ],
    jobs=[hourly_assets_job],
    schedules=[hourly_schedule],
    sensors=[
        retry_stale_failed_partitions,
    ]
)



