# assets_bronze2.py

from .factories.assets_ext_to_bronze_factory import make_ext_to_bronze_asset

assets = [
    make_ext_to_bronze_asset(
        exchange="kraken",
        ext_table="bronze_ohlcv_ext",
        native_table="bronze_ohlcv_native",
    ),
    make_ext_to_bronze_asset(
        exchange="binance",
        ext_table="bronze_ohlcv_ext",
        native_table="bronze_ohlcv_native",
    ),
]