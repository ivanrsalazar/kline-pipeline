## Warehouse Layers

### Bronze
Raw OHLCV data from WebSocket + REST sources.
May contain duplicates or partial candles.

### Silver
Canonical 1-minute OHLCV fact table.
Deduplicated, finalized, partitioned by hour.

### Gold
Derived aggregates (5m, 15m, 30m, 1h).
Used for analytics, dashboards, and models.