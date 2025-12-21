# kline_pipeline/jobs.py

from dagster import define_asset_job
from .partitions import hourly_partitions

hourly_assets_job = define_asset_job(
    name="hourly_assets_job",
    partitions_def=hourly_partitions,
)