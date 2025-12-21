# ingestion/exchanges/binance.py

import json
import websockets
import ssl
import certifi
from .base import ExchangeClient

class BinanceClient(ExchangeClient):
    def __init__(self, symbol: str, interval: int):
        self.symbol = symbol.lower()
        self.interval = f"{interval}m"
        self.uri = f"wss://stream.binance.us:9443/ws/{self.symbol}@kline_{self.interval}"
        self.websocket = None
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.load_verify_locations(certifi.where())

    async def connect(self):
        self.websocket = await websockets.connect(
            self.uri, ssl=self.ssl_context, ping_timeout=60
        )

    async def subscribe(self):
        # Binance auto-subscribes via URI
        pass

    async def messages(self):
        async for msg in self.websocket:
            yield json.loads(msg)
