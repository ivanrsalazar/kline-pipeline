# assets_silver.py

from .asset_config import EXCHANGES
from .asset_config import target_tokens
from .factories.assets_silver_factory import make_silver_asset

silver_assets = []
pairs = {f"{token.upper()}-USD" for token in target_tokens}
for symbol in pairs:
    exchanges = [
        ex for ex, cfg in EXCHANGES.items()
        if symbol in cfg["symbols"]
    ]
    silver_assets.append(make_silver_asset(symbol, exchanges))