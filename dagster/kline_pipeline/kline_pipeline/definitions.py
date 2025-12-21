# kline_pipeline/definitions.py

from dagster import Definitions

from .assets_bronze import bronze_ohlcv_native
from .assets_silver import fact_ohlcv_kraken_eth_1m
from .jobs import hourly_assets_job
from .schedules import hourly_schedule

defs = Definitions(
    assets=[
        bronze_ohlcv_native,
        fact_ohlcv_kraken_eth_1m,
    ],
    jobs=[hourly_assets_job],
    schedules=[hourly_schedule],
)