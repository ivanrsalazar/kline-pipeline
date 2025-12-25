# asset_config.py

EXCHANGES = {
    "kraken": {
        "symbols": {
            "ETH-USD": {
                "rest_pair": "ETH/USD",
                "interval_seconds": 60,
            },
            "SOL-USD": {
                "rest_pair": "SOL/USD",
                "interval_seconds": 60,
            },
        }
    },
    "binance": {
        "symbols": {
            "ETHUSDT": {
                "interval_seconds": 60,
            },
            "SOLUSDT": {
                "interval_seconds": 60,
            },
        }
    },
}