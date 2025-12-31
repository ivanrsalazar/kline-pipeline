# kline_pipeline/definitions.py
from dagster import Definitions, build_schedule_from_partitioned_job

# --------------------
# ASSETS (v1)
# --------------------
from .assets_bronze import bronze_ohlcv_native
from .assets_bronze_binance import bronze_ohlcv_binance_1m
from .assets_bronze_binance_rest import bronze_ohlcv_binance_rest_1m
from .assets_bronze_kraken_rest import bronze_ohlcv_kraken_rest_1m
from .assets_silver import fact_ohlcv_eth_1m
from .dummy_asset import dummy_asset


# --------------------
# ASSETS (v2 / factory)
# --------------------
from .assets_bronze2 import assets as bronze_ext_assets_v2
from .assets_bronze_rest import bronze_rest_assets
from .assets_silver2 import silver_assets

# Gold
from .gold.ohlcv_5m import gold_ohlcv_5m
from .gold.ohlcv_15m import gold_ohlcv_15m
from .gold.ohlcv_30m import gold_ohlcv_30m
from .gold.ohlcv_1h import gold_ohlcv_1h

# --------------------
# JOBS
# --------------------
from .jobs import hourly_assets_job
from .jobs_v2 import v2_hourly_assets_job

# --------------------
# SCHEDULES
# --------------------
hourly_schedule = build_schedule_from_partitioned_job(
    job=hourly_assets_job
)

v2_hourly_schedule = build_schedule_from_partitioned_job(
    job=v2_hourly_assets_job
)

# --------------------
# SENSORS
# --------------------
from .sensors import backfill_missing_ohlcv_partitions

# --------------------
# DEFINITIONS
# --------------------
defs = Definitions(
    assets=[
        # ---------- v1 ----------
        bronze_ohlcv_binance_1m,
        bronze_ohlcv_binance_rest_1m,
        bronze_ohlcv_kraken_rest_1m,
        bronze_ohlcv_native,
        fact_ohlcv_eth_1m,
        dummy_asset,

        # ---------- v2 ----------
        *bronze_ext_assets_v2,
        *bronze_rest_assets,
        *silver_assets,

        # gold
        gold_ohlcv_5m,
        gold_ohlcv_15m,
        gold_ohlcv_30m,
        gold_ohlcv_1h,

    ],
    jobs=[
        hourly_assets_job,
        v2_hourly_assets_job,
    ],
    schedules=[
        hourly_schedule,
        v2_hourly_schedule,
    ],
    sensors=[
        backfill_missing_ohlcv_partitions
    ],
)