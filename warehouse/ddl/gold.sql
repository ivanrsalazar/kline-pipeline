CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.ohlcv_5m (
    exchange         text,
    symbol           text,
    interval_start   timestamptz,
    open             double precision,
    high             double precision,
    low              double precision,
    close            double precision,
    volume           double precision,
    trade_count      integer
)
PARTITION BY RANGE (interval_start);

CREATE TABLE IF NOT EXISTS gold.ohlcv_15m (
    LIKE silver.fact_ohlcv
    INCLUDING DEFAULTS
    INCLUDING CONSTRAINTS
    INCLUDING GENERATED
);

-- Optional but recommended
ALTER TABLE gold.ohlcv_15m
    ALTER COLUMN interval_seconds SET DEFAULT 900;

CREATE UNIQUE INDEX IF NOT EXISTS ohlcv_15m_uniq
ON gold.ohlcv_15m (
    exchange,
    symbol,
    interval_start
);


CREATE TABLE IF NOT EXISTS gold.ohlcv_30m (
    LIKE silver.fact_ohlcv
    INCLUDING DEFAULTS
    INCLUDING CONSTRAINTS
    INCLUDING GENERATED
);

ALTER TABLE gold.ohlcv_30m
    ALTER COLUMN interval_seconds SET DEFAULT 1800;

CREATE UNIQUE INDEX IF NOT EXISTS ohlcv_30m_uniq
ON gold.ohlcv_30m (
    exchange,
    symbol,
    interval_start
);


CREATE TABLE IF NOT EXISTS gold.ohlcv_1h (
    LIKE silver.fact_ohlcv
    INCLUDING DEFAULTS
    INCLUDING CONSTRAINTS
    INCLUDING GENERATED
);

ALTER TABLE gold.ohlcv_1h
    ALTER COLUMN interval_seconds SET DEFAULT 3600;

CREATE UNIQUE INDEX IF NOT EXISTS ohlcv_1h_uniq
ON gold.ohlcv_1h (
    exchange,
    symbol,
    interval_start
);