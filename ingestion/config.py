# ingestion/config.py

from dataclasses import dataclass

@dataclass
class TradingPairConfig:
    exchange: str
    symbol: str
    interval_minutes: int
    output_dir: str


PAIRS = [
    TradingPairConfig(
        exchange="binance",
        symbol="ETHUSDT",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/binance/ethusdt"
    ),
    TradingPairConfig(
        exchange="kraken",
        symbol="ETH/USD",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/kraken/ethusd_eng"
    ),
]
