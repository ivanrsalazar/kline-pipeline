# kline_pipeline/schedules.py

from dagster import build_schedule_from_partitioned_job
from .jobs import hourly_assets_job

hourly_schedule = build_schedule_from_partitioned_job(
    job=hourly_assets_job
)