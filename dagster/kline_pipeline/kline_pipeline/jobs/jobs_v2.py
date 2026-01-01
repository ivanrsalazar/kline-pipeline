# kline_pipeline/jobs_v2.py

from dagster import define_asset_job, AssetSelection
from kline_pipeline.schedules.partitions import hourly_partitions


v2_hourly_assets_job = define_asset_job(
    name="v2_hourly_assets_job",
    partitions_def=hourly_partitions,
    selection=(
        # ---- EXT → BRONZE (native bronze v2)
        AssetSelection.groups("dummy") |

        # ---- EXT → BRONZE (native bronze v2)
        AssetSelection.groups("bronze_native_v2") |

        # ---- REST Backfill -> BRONZE (native bronze v2)
        AssetSelection.groups("bronze_rest_v2") |
        # ---- SILVER
        AssetSelection.groups("silver_v2") |
        # ---- GOLD
        AssetSelection.groups("gold_v2") |

        AssetSelection.groups("final_message")
    ),
)