CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.fact_ohlcv (
    exchange         text        NOT NULL,
    symbol           text        NOT NULL,
    quote_asset      text        NOT NULL,
    interval_seconds integer     NOT NULL,
    interval_start   timestamptz NOT NULL,
    interval_end     timestamptz NOT NULL,
    open             double precision,
    high             double precision,
    low              double precision,
    close            double precision,
    volume           double precision,
    quote_volume     double precision,
    trade_count      integer,
    vwap             double precision,
    source           text        NOT NULL,
    ingestion_ts     timestamptz NOT NULL
)
PARTITION BY RANGE (interval_start);