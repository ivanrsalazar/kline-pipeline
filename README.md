## Crypto Kline Data Pipeline

This project aims to ingest crypto kline data capturing the open, close, high, low, and volume of 1m interval candles from multiple exchanges and multiple trading pairs.

Architecture:
- Raw Data Ingestion Engine
    - `ingestion/engine.py` takes the provided exchanges and trading pairs from `ingestion/config.py` and establishes a web socket connection for each exchanges/trading pairs. Candle events are written locally as well as directly into S3 with append only JSONL files. Allows for scaling the number of trading pairs data is being captured from a certain exchange.
    - JSONL files are stored in the `kline-pipeline-bronze` S3 bucket which uses a HIVE style naming convention for the "directories"/file prefixes

- Dagster
    - The Dagster assets are created using factories for each layer of the data warehouse ingestion process. 
    - Assets responsible for uploads into the `bronze.bronze_ohclv_native` table are created via the `assets_ext_to_bronze_factory.py`
    - These assets are created for each exchange/trading pair combo 
    - After the websocket data has been ingested, there is a REST backfill layer that fills in the missing minutes via a REST source for each of the respective exchanges. 
    - These assets are also split by exchange/trading pair combos and are created via the `assets_bronze_rest_factory.py`
    - The last layer of the ingestion process is the merge from the bronze table into the silver fact table, `silver.fact_ohlcv`.
    - This layer is trading pair specific and exchange agnostic.
    - These assets are created via the assets_silver_factory.py
    - This approach also allows for scaling the how many assets can be created to support the ingestion process.

- Hosting
    - The Raw Data Ingestion Engine and Dagster server processes take place in a VM hosted on EC2, which also hosts the data warehouse on a PostgreSQL server.
    - For reference the VM is an Ubuntu t3.medium

- Data Model
    - As stated above, the data being captured is Open, High, Low, Close, and Volume for each of the 1m intervals. 
    - The data is split into two tables, a bronze table which holds the following columns:
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

    - The table is meant to store data that can contain duplicates and unfinished minute intervals

    - The silver fact table is meant to merge from this bronze table and only take the finished minute interval rows and drops all the duplicates, `silver.fact_ohlcv`
    - The silver factor table holds the following columns:
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
