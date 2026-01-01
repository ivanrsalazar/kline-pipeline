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

Architecture:
- Raw Data Ingestion Engine
    - `ingestion/engine.py` takes the provided exchanges and trading pairs from `ingestion/config.py` and establishes a web socket connection for each exchanges/trading pairs. 
    - Candle events are written locally as well as directly into S3 with append only JSONL files. 
    - Allows for scaling the number of trading pairs supported for each exchange
    - JSONL files are stored in the `kline-pipeline-bronze` S3 bucket which uses a HIVE style naming convention for the "directories"/file prefixes
    - Data is uploaded into a new file prefix every hour

- Dagster (Orchestration)
    - Bronze Websocket Data Ingestion
        - The Dagster assets are created using factories for each layer of the data warehouse ingestion process. 
        - Assets responsible for uploads into the `bronze.bronze_ohclv_native` table are created via the `assets_ext_to_bronze_factory.py`
        - These assets are created for each exchange/trading pair combo 
    - REST Backfill
        - After the websocket data has been ingested, there is a REST backfill layer that fills in the missing minutes via a REST source for each of the respective exchanges 
        - These assets are also split by exchange/trading pair combos and are created via the `assets_bronze_rest_factory.py`
    - Silver / Fact 
        - The last layer of the ingestion process is the merge from the bronze table into the silver fact table, `silver.fact_ohlcv`
        - This layer is trading pair specific and exchange agnostic
        - These assets are created via the assets_silver_factory.py
    - This approach allows for scaling the number of trading pairs that can be ingested into the data warehouse
    - Ingestion occurs hourly

- Hosting
    - The Raw Data Ingestion Engine and Dagster server processes take place in a VM hosted on EC2, which also hosts the data warehouse on a PostgreSQL server.
    - For reference the VM is an Ubuntu t3.medium

- Data Model
    - As stated above, the data being captured is Open, High, Low, Close, and Volume for each of the 1m intervals. 
    - The data is split into two tables, a bronze and silver layer.
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
    - <img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/part_issues.png?raw=true">
    - This image shows how easily Web Socket issues can be spotted using stacked bar chart
    - <img src="https://github.com/ivanrsalazar/kline-pipeline/blob/main/docs/total_hourly_stacked.png?raw=true">
    - As the number of trading pairs supported increased, having a chart for each one is no longer scalabe
    - The series of charts were replaced with a completion rate bar chart which counts the number of ingested minutes divided by the expected number of ingested minutes    


    
