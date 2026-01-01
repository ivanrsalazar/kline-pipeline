# kline_pipeline/definitions.py
from dagster import Definitions, build_schedule_from_partitioned_job

# --------------------
# ASSETS (v2 / factory)
# --------------------
from .assets_bronze2 import assets as bronze_ext_assets_v2
from .assets_bronze_rest import bronze_rest_assets
from .assets_silver2 import silver_assets
from .dummy_asset import dummy_asset

# Gold
from .gold.ohlcv_5m import gold_ohlcv_5m
from .gold.ohlcv_15m import gold_ohlcv_15m
from .gold.ohlcv_30m import gold_ohlcv_30m
from .gold.ohlcv_1h import gold_ohlcv_1h

# Slack
from .asset_final_message import final_message

# --------------------
# JOBS
# --------------------
from .jobs_v2 import v2_hourly_assets_job


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

        # slack
        final_message,


    ],
    jobs=[
        v2_hourly_assets_job,
    ],
    schedules=[
        v2_hourly_schedule,
    ],
    sensors=[
        backfill_missing_ohlcv_partitions
    ],
)