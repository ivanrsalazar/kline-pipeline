# kline_pipeline/partitions.py

from dagster import HourlyPartitionsDefinition

hourly_partitions = HourlyPartitionsDefinition(
    start_date="2025-12-01-00:00",
    timezone="UTC",
)