# assets_bronze2.py

from ..factories.assets_ext_to_bronze_factory import make_ext_to_bronze_asset


def extract_symbol_from_prefix(s3_prefix: str) -> str:
    """
    Extract symbol from hive-style S3 prefix.

    Example:
        exchange=kraken/symbol=ETH-USD/interval_minutes=1
        -> ethusd
    """
    for part in s3_prefix.split("/"):
        if part.startswith("symbol="):
            raw = part.split("=", 1)[1]
            return raw.lower().replace("-", "").replace("/", "")
    raise ValueError(f"Could not extract symbol from prefix: {s3_prefix}")


# -------------------------------------------------
# Configuration (single source of truth)
# -------------------------------------------------
BRONZE_SOURCES = []
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
for exchange in ['binance', 'kraken']:
    for token in target_tokens:
        if exchange == 'binance':
            src = (f"{exchange}",f"exchange={exchange}/symbol={token.upper()}USDT/interval_minutes=1")
        else:
            src = (f"{exchange}", f"exchange={exchange}/symbol={token.upper()}-USD/interval_minutes=1")
        BRONZE_SOURCES.append(src)



# -------------------------------------------------
# Asset generation
# -------------------------------------------------
assets = []

for exchange, s3_prefix in BRONZE_SOURCES:
    symbol = extract_symbol_from_prefix(s3_prefix)

    assets.append(
        make_ext_to_bronze_asset(
            exchange=exchange,
            symbol=symbol,        # 👈 pass symbol explicitly
            s3_prefix=s3_prefix,
        )
    )