-- One row per equity instrument per trading day.
-- Prices are stored as IEEE 754 doubles converted from Databento fixed-point (int64 / 1e9).
CREATE TABLE IF NOT EXISTS equity_ohlcv_1d (
    ts_event      TIMESTAMPTZ      NOT NULL,
    symbol        TEXT             NOT NULL,
    instrument_id BIGINT,
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        BIGINT,
    PRIMARY KEY (ts_event, symbol)
);
