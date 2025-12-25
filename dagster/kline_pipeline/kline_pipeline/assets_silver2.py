# assets_silver.py

from .asset_config import EXCHANGES
from .factories.assets_silver_factory import make_silver_asset

silver_assets = []

for symbol in {"ETH-USD", "SOL-USD"}:
    exchanges = [
        ex for ex, cfg in EXCHANGES.items()
        if symbol in cfg["symbols"]
    ]
    silver_assets.append(make_silver_asset(symbol, exchanges))