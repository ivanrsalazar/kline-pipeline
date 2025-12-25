# kline_pipeline/schedules_v2.py

from dagster import build_schedule_from_partitioned_job

from .jobs_v2 import v2_hourly_assets_job

# This schedule is derived directly from the job's partitions.
# No cron expression is used — Dagster handles partition alignment safely.
v2_hourly_schedule = build_schedule_from_partitioned_job(
    job=v2_hourly_assets_job
)