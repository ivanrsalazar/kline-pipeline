EXCHANGES = {"binance": {"symbols": {}}, "kraken": {"symbols": {}}}

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


for exchange in ["binance", "kraken"]:
    symbols = {}
    for token in target_tokens:
        if exchange == "binance":
            if token == "rekt":
                symbols[f"{token.upper()}-USD"] = {
                "rest_pair": f"1000{token.upper()}USDT",
                "ws_pair": f"1000{token}usdt",
                }
            else:
                symbols[f"{token.upper()}-USD"] = {
                    "rest_pair": f"{token.upper()}USDT",
                    "ws_pair": f"{token}usdt",
                }
        else:
            symbols[f"{token.upper()}-USD"] = {
                "rest_pair": f"{token.upper()}/USD",
                "ws_pair": f"{token}/usd",
            }

    EXCHANGES[exchange]["symbols"] = symbols
