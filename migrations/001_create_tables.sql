-- One row per option contract per trading day.
-- Prices are stored as IEEE 754 doubles converted from Databento fixed-point (int64 / 1e9).
CREATE TABLE IF NOT EXISTS option_ohlcv_1d (
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

-- Tracks which .dbn.zst files have been loaded so that re-runs are idempotent.
CREATE TABLE IF NOT EXISTS _loaded_files (
    file_path TEXT        PRIMARY KEY,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    row_count INT
);
