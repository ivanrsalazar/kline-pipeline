## Crypto Kline Data Pipeline

This project aims to ingest crypto kline data capturing the open, close, high, low, and volume of 1m interval candles from multiple exchanges and multiple trading pairs.

### Motivation

This project was built to explore the challenges of real-time market data ingestion:
- Late-arriving data
- Missing intervals
- Exchange inconsistencies between WebSocket and REST APIs
- Idempotent backfills
- Warehouse partitioning at scale

The goal is to build a production-style pipeline that is correct, observable, and extensible.


### Data Flow
<img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/data_flow.png?raw=true">


Architecture:
- Raw Data Ingestion Engine
    - `ingestion/engine.py` takes the provided exchanges and trading pairs from `ingestion/config.py` and establishes a web socket connection for each exchanges/trading pairs. 
    - Candle events are written locally as well as directly into S3 with append only JSONL files. 
    - Allows for scaling the number of trading pairs supported for each exchange
    - JSONL files are stored in the `kline-pipeline-bronze` S3 bucket which uses a HIVE style naming convention for the "directories"/file prefixes
        - `kline-pipeline-bronze/exchange={exchange}/symbol={symbol}/interval_minutes=1/year={year}/month={month}/day={day}/hour={hour}`
    - Data is uploaded into a new file prefix every hour
    - Supported Trading Pairs
        - ETH/USD
        - SOL/USD
        - BTC/USD
        - LINK/USD
        - APE/USD
        - DOT/USD
        - PEPE/USD
        - TRUMP/USD
        - PUMP/USD
        - JUP/USD
        - PENGU/USD
        - REKT/USD
        - FARTCOIN/USD
        - WIF/USD
        - KSM/USD
    - Exchanges Supported
        - Binance US
        - Kraken

- Dagster (Orchestration)
    - Bronze Websocket Data Ingestion
        - The Dagster assets are created using factories for each layer of the data warehouse ingestion process. 
        - Assets responsible for uploads into the `bronze.bronze_ohclv_native` table are created via the `assets_ext_to_bronze_factory.py`
        - These assets are created for each exchange/trading pair combo 
    - REST Backfill
        - After the websocket data has been ingested, there is a REST backfill layer that fills in the missing minutes via a REST source for each of the respective exchanges 
        - These assets are also split by exchange/trading pair combos and are created via the `assets_bronze_rest_factory.py`
    - Silver / Fact 
        - The penultimate layer of the ingestion process is the merge from the bronze table into the silver fact table, `silver.fact_ohlcv`
        - This layer is trading pair specific and exchange agnostic
        - These assets are created via the assets_silver_factory.py
    - Gold Derived tables
        - The last layer of assets are responsible for deriving the larger time intervals from the silver fact table
            - 5m
            - 15m
            - 30m
            - 1h
        - Here's a quick merge query for the 5m interval
        ```sql
        INSERT INTO gold.ohlcv_5m
        SELECT
            exchange,
            symbol,
            bucket_start AS interval_start,
            bucket_start + interval '5 minutes' AS interval_end,

            (array_agg(open  ORDER BY interval_start))[1]        AS open,
            max(high)                                            AS high,
            min(low)                                             AS low,
            (array_agg(close ORDER BY interval_start DESC))[1]   AS close,

            sum(volume)        AS volume,
            sum(quote_volume)  AS quote_volume,
            sum(trade_count)   AS trade_count,

            'derived'          AS source,
            max(ingestion_ts)  AS ingestion_ts
        FROM (
            SELECT
                exchange,
                symbol,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count,
                ingestion_ts,
                interval_start,
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
    - This approach allows for scaling the number of trading pairs that can be ingested into the data warehouse
    - Ingestion occurs hourly

- Hosting
    - The Raw Data Ingestion Engine and Dagster server processes take place in a VM hosted on EC2, which also hosts the data warehouse on a PostgreSQL server.
    - For reference the VM is an Ubuntu t3.medium

- Data Model
    - As stated above, the data being captured is Open, High, Low, Close, and Volume for each of the 1m intervals. 
    - The data is split into six tables, a bronze table, a silver fact table, and four derived gold tabls.
    - `bronze.bronze_ohlcv_native` stores data that can contain duplicates and unfinished minute intervals: 
        - exchange (text)
        - symbol (text)
        - interval_seconds (integer)
        - interval_start (timestampz)
        - interval_end (timestampz)
        - open (double)
        - high (double)
        - low (double)
        - close (double)
        - volume (double)
        - vwap (double) [Volume Weighted Adjusted Price]
        - trade_count (integer)
        - source (text) [Websocket or REST]
        - ingestion_ts (timestampz)

   
    - `silver.fact_ohlcv`, the silver fact table is meant to merge from the bronze table and only take the finished minute intervals and drops all the duplicates:
        - exchange (text)
        - symbol (text)
        - quote_asset (text)
        - interval_seconds (integer)
        - interval_start (timestampz)
        - interval_end (timestampz)
        - open (double)
        - high (double)
        - low (double)
        - close (double)
        - volume (double)
        - quote_volume (double)
        - trade_count (integer)
        - vwap (double) [Volume Weighted Adjusted Price]
        - source (text) [Websocket or REST]
        - ingestion_ts (timestampz)
    - `gold.ohlcv_{interval}`, the gold derived tables are for larger time intervals using the silver fact table
        - exchange (text)
        - symbol (text)
        - interval_start (timestampz)
        - interval_end (timestampz)
        - open (double)
        - high (double)
        - low (double)
        - close (double)
        - volume (double)
        - quote_volume (double)
        - trade_count (integer)
        - source (text) [Websocket or REST]
        - ingestion_ts (timestampz)
    - Data Guarantees
        - Exactly one candle per exchange / symbol / minute in silver
        - Late-arriving data is merged correctly
        - REST backfills do not overwrite WebSocket data
        - Silver table is partitioned by hour for efficient backfills
        - Aggregates are derived strictly from silver 1m
        - All merges are idempotent
    - Design Trade-offs
        - PostgreSQL is used instead of BigQuery/Snowflake to keep costs low
        - Hourly partitions were chosen over daily partitions to support late REST backfills
        - Bronze is append-only to preserve raw ingestion history

- Monitoring
    - Data is connected to Looker Studio for dashboard monitoring
    - Includes hourly completeness bar charts for each exchange / trading pair
    - <img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/dashboards/looker/part_issues.png?raw=true">
    - This image shows how easily Web Socket issues can be spotted using stacked bar chart
    - <img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/dashboards/looker/total_hourly_stacked.png?raw=true">
    - As the number of trading pairs supported increased, having a chart for each one is no longer scalabe
    - The series of charts were replaced with a completion rate bar chart which counts the number of ingested minutes divided by the expected number of ingested minutes    


    
