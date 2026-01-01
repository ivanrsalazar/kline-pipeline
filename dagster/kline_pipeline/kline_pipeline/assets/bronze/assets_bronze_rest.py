# assets_bronze_rest.py

from ..asset_config import EXCHANGES
from ..factories.assets_bronze_rest_factory import make_bronze_rest_asset

bronze_rest_assets = []

for exchange, cfg in EXCHANGES.items():
    for symbol, sym_cfg in cfg["symbols"].items():
        bronze_rest_assets.append(
            make_bronze_rest_asset(
                exchange=exchange,
                symbol=symbol,
                rest_pair=sym_cfg.get("rest_pair"),
            )
        )