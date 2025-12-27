from .factories.assets_ext_to_bronze_factory import make_ext_to_bronze_asset

assets = []

assets.append(
    make_ext_to_bronze_asset(
        exchange="kraken",
        s3_prefix="exchange=kraken/symbol=ETH-USD/interval_minutes=1",
    )
)

assets.append(
    make_ext_to_bronze_asset(
        exchange="binance",
        s3_prefix="exchange=binance/symbol=ETHUSDT/interval_minutes=1",
    )
)