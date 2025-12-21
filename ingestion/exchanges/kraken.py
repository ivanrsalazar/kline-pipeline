# ingestion/exchanges/kraken.py

import json
import websockets
import ssl
import certifi
from .base import ExchangeClient

class KrakenClient(ExchangeClient):
    URI = "wss://ws.kraken.com/v2"

    def __init__(self, symbol: str, interval: int):
        self.symbol = symbol
        self.interval = interval
        self.websocket = None
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.load_verify_locations(certifi.where())

    async def connect(self):
        self.websocket = await websockets.connect(
            self.URI, ssl=self.ssl_context, ping_timeout=60
        )

    async def subscribe(self):
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "symbol": [self.symbol],
                "interval": self.interval,
                "snapshot": True,
            }
        }
        await self.websocket.send(json.dumps(msg))

    async def messages(self):
        async for msg in self.websocket:
            data = json.loads(msg)
            if isinstance(data, dict) and data.get("channel") == "heartbeat":
                continue
            yield data
