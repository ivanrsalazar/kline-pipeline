# Crypto Kline Data Pipeline

A production-style data pipeline for ingesting, validating, and aggregating real-time cryptocurrency OHLCV (open, high, low, close, volume) data at 1-minute granularity across multiple exchanges and trading pairs.

---

## Motivation

This project was built to explore real-world challenges in **real-time market data ingestion**, including:

- Late-arriving data  
- Missing intervals  
- Idempotent backfills  
- High-cardinality warehouse partitioning  
- Observability and data correctness at scale  

The goal is to design a pipeline that is **correct, scalable, observable, and extensible**, while remaining cost-conscious.

---

## High-Level Data Flow

<img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/data_flow.png?raw=true">

---

## Architecture Overview

### Raw Data Ingestion (WebSocket Engine)

<img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/raw_data_ingestion.png?raw=true">

#### Responsibilities
- Establishes WebSocket connections per exchange and trading pair
- Captures 1-minute candle events in real time
- Writes append-only JSONL files locally and to S3
- Rotates files hourly using Hive-style partitioned prefixes

#### Implementation
- `ingestion/engine.py`
- Configuration via `ingestion/config.py`
- Horizontally scalable by adding exchanges or trading pairs

#### S3 Layout (Hive-style)
```
kline-pipeline-bronze/
exchange={exchange}/
symbol={symbol}/
interval_minutes=1/
year={year}/month={month}/day={day}/hour={hour}/
```

#### Notes
- Trading pairs may use stablecoin bases (USDT / USDC)
- Symbols are normalized to USD (e.g. `BTC-USD`)
- One JSONL file per symbol per hour

#### Supported Exchanges
- Binance US
- Kraken

#### Supported Symbols
- BTC-USD
- ETH-USD
- SOL-USD
- LINK-USD
- DOT-USD
- APE-USD
- PEPE-USD
- TRUMP-USD
- JUP-USD
- PUMP-USD
- PENGU-USD
- REKT-USD
- FARTCOIN-USD
- WIF-USD
- KSM-USD

---

## Orchestration (Dagster)

<img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/dagster_orchestration.png?raw=true">

All warehouse ingestion and transformations are orchestrated hourly using Dagster.

### Asset Layers

#### 1. Sleeper Asset
- Delays execution for the first 3 minutes of the hour
- Broadcasts the active partition window to Slack

#### 2. Bronze Ingestion (WebSocket)
- Assets generated via `assets_ext_to_bronze_factory.py`
- One asset per exchange / trading pair
- Loads raw WebSocket data into `bronze.bronze_ohlcv_native`

#### 3. REST Backfill
- Assets generated via `assets_bronze_rest_factory.py`
- Fills missing minutes using REST APIs
- Runs after WebSocket ingestion
- Never overwrites newer WebSocket data

#### 4. Silver (Fact Table)
- Assets generated via `assets_silver_factory.py`
- Exchange-agnostic, symbol-specific
- Merges bronze data into a canonical fact table
- Deduplicates records
- Guarantees exactly one candle per minute
- Emits Slack alerts for missing minutes

#### 5. Gold (Derived Tables)
- Aggregates silver 1m candles into:
  - 5m
  - 15m
  - 30m
  - 1h
- Strictly derived from silver (no raw data leakage)

Example 5-minute aggregation:

```sql
INSERT INTO gold.ohlcv_5m
SELECT
    exchange,
    symbol,
    bucket_start AS interval_start,
    bucket_start + interval '5 minutes' AS interval_end,

    (array_agg(open  ORDER BY interval_start))[1]      AS open,
    max(high)                                          AS high,
    min(low)                                           AS low,
    (array_agg(close ORDER BY interval_start DESC))[1] AS close,

    sum(volume)       AS volume,
    sum(quote_volume) AS quote_volume,
    sum(trade_count)  AS trade_count,

    'derived'         AS source,
    max(ingestion_ts) AS ingestion_ts
FROM (
    SELECT *,
           date_trunc('hour', interval_start)
           + floor(extract(minute from interval_start) / 5) * interval '5 minutes'
           AS bucket_start
    FROM silver.fact_ohlcv
) s
GROUP BY exchange, symbol, bucket_start
ON CONFLICT (exchange, symbol, interval_start)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    quote_volume = EXCLUDED.quote_volume,
    trade_count = EXCLUDED.trade_count,
    ingestion_ts = EXCLUDED.ingestion_ts;
```


#### 6. Slack Reporting
- Sends success confirmation per hourly run
- Alerts only when data completeness issues occur

---

### Hosting

- All services run on a single EC2 instance:
    - WebSocket ingestion engine
    - Dagster daemon & UI
    - PostgreSQL warehouse
-   Instance: Ubuntu t3.medium

---

### Data Model

#### Bronze: bronze.bronze_ohlcv_native

- Append-only raw ingestion table.
- May contain duplicates and incomplete minutes.

#### Silver: silver.fact_ohlcv

- Canonical 1-minute fact table.
	- Deduplicated
	- Late data merged safely
	- Hourly partitioned

#### Gold: gold.ohlcv_{interval}

- Analytical tables at larger intervals (5m, 15m, 30m, 1h).

---

#### Data Guarantees
- Exactly one candle per exchange / symbol / minute in silver
- Late REST backfills merge correctly
- REST never overwrites newer WebSocket data
- Hourly partitions enable efficient reprocessing
- Aggregations derived strictly from silver
- All merges are idempotent

---

#### Monitoring & Analytics
- Looker Studio dashboards for ingestion health
- Hourly completeness checks
- Exchange-level price and volume analytics

--- 

#### Known Limitations
- Single PostgreSQL instance lacks fault tolerance
- Storage and compute are co-located
- Scaling requires migration to cloud-native warehousing

--- 

#### Future Improvements
- Migrate storage to GCS
- Load bronze directly from object storage
- Move warehouse to BigQuery
- Introduce dbt for silver/gold transformations

