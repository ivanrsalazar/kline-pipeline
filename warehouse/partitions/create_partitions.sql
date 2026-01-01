DO $$
DECLARE
    start_ts timestamptz := date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '24 hours';
    end_ts   timestamptz := date_trunc('hour', now() AT TIME ZONE 'UTC') + interval '72 hours';
    t        timestamptz;

    bronze_part text;
    silver_part text;
BEGIN
    t := start_ts;

    WHILE t < end_ts LOOP
        -- ============================
        -- Canonical partition names
        -- ============================
        bronze_part := format(
            'bronze_ohlcv_native_%s',
            to_char(t, 'YYYY_MM_DD_HH24')
        );

        silver_part := format(
            'fact_ohlcv_%s',
            to_char(t, 'YYYY_MM_DD_HH24')
        );

        -- ============================
        -- BRONZE partition
        -- ============================
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS bronze.%I
             PARTITION OF bronze.bronze_ohlcv_native
             FOR VALUES FROM (%L) TO (%L)',
            bronze_part,
            t,
            t + interval '1 hour'
        );

        -- ============================
        -- SILVER partition
        -- ============================
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS silver.%I
             PARTITION OF silver.fact_ohlcv
             FOR VALUES FROM (%L) TO (%L)',
            silver_part,
            t,
            t + interval '1 hour'
        );

        -- ============================
        -- Unique index (silver only)
        -- ============================
        EXECUTE format(
            'CREATE UNIQUE INDEX IF NOT EXISTS %I
             ON silver.%I (
                 exchange,
                 symbol,
                 interval_seconds,
                 interval_start
             )',
            silver_part || '_uniq_idx',
            silver_part
        );

        t := t + interval '1 hour';
    END LOOP;
END $$;