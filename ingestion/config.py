# ingestion/config.py

from dataclasses import dataclass

@dataclass
class TradingPairConfig:
    exchange: str
    symbol: str
    interval_minutes: int
    output_dir: str


PAIRS = [
    
    # ETH USD 
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

    # SOL USD 
    TradingPairConfig(
        exchange="binance",
        symbol="SOLUSDT",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/binance/solusdt"
    ),
    TradingPairConfig(
        exchange="kraken",
        symbol="SOL/USD",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/kraken/solusd_eng"
    ),

    # BTC USD 
    TradingPairConfig(
        exchange="binance",
        symbol="BTCUSDT",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/binance/btcusdt"
    ),
    TradingPairConfig(
        exchange="kraken",
        symbol="BTC/USD",
        interval_minutes=1,
        output_dir="/home/ubuntu/data/kraken/btcusd_eng"
    ),
]
