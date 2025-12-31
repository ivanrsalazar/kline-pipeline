# ingestion/config.py

from dataclasses import dataclass

@dataclass
class TradingPairConfig:
    exchange: str
    symbol: str
    interval_minutes: int
    output_dir: str

PAIRS = []

target_tokens = [
    "eth",
    "sol",
    "btc",
    "link",
    "ape",
    "dot",
    "pepe",
    "trump",
    "pump",
    "jup",
    "pengu",
    "rekt",
    "fartcoin",
    "wif",
    "ksm",

]


for token in target_tokens:
    if token == 'rekt':
        PAIRS.append(
            TradingPairConfig(
                exchange="binance",
                symbol=f"1000{token.upper()}USDT",
                interval_minutes=1,
                output_dir=f"/home/ubuntu/data/binance/{token}usdt"
            )
            
        )
    else:
        PAIRS.append(
            TradingPairConfig(
                exchange="binance",
                symbol=f"{token.upper()}USDT",
                interval_minutes=1,
                output_dir=f"/home/ubuntu/data/binance/{token}usdt"
            )
            
        )
    PAIRS.append(
        TradingPairConfig(
            exchange="kraken",
            symbol=f"{token.upper()}/USD",
            interval_minutes=1,
            output_dir=f"/home/ubuntu/data/kraken/{token}usd_eng"
        )
    )
