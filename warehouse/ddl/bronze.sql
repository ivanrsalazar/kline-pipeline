CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.bronze_ohlcv_native (
    exchange         text        NOT NULL,
    symbol           text        NOT NULL,
    interval_seconds integer     NOT NULL,
    interval_start   timestamptz NOT NULL,
    interval_end     timestamptz NOT NULL,
    open             double precision,
    high             double precision,
    low              double precision,
    close            double precision,
    volume           double precision,
    vwap             double precision,
    trade_count      integer,
    source           text        NOT NULL, -- ws | rest
    ingestion_ts     timestamptz NOT NULL
)
PARTITION BY RANGE (interval_start);

