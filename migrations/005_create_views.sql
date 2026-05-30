-- Parsed option chain view: extracts structured fields from OCC symbol format.
-- OCC format: 6-char root (space-padded) + 6-char YYMMDD expiry + C/P + 8-char strike×1000
CREATE OR REPLACE VIEW option_ohlcv_1d_v AS
SELECT
    ts_event,
    ts_event::date                              AS trade_date,
    symbol,
    trim(substr(symbol, 1, 6))                  AS root_symbol,
    to_date(substr(symbol, 7, 6), 'YYMMDD')    AS expiry,
    substr(symbol, 13, 1)                       AS option_type,
    substr(symbol, 14, 8)::numeric / 1000.0     AS strike,
    instrument_id,
    open,
    high,
    low,
    close,
    volume,
    to_date(substr(symbol, 7, 6), 'YYMMDD') - ts_event::date AS dte
FROM option_ohlcv_1d;

-- Enriched option chain: joins parsed options with underlying equity OHLCV.
CREATE OR REPLACE VIEW option_chain_1d AS
SELECT
    o.ts_event::date                            AS trade_date,
    o.root_symbol                               AS symbol,
    o.expiry,
    o.option_type,
    o.strike,
    o.dte,

    -- underlying
    e.close                                     AS close_underlying,
    e.high - e.low                              AS range_underlying,
    e.volume                                    AS volume_underlying,

    -- moneyness
    e.close - o.strike                          AS moneyness,
    (e.close - o.strike) / e.close * 100        AS moneyness_pct,
    ln(o.strike / e.close)                      AS log_moneyness,

    -- option OHLCV
    o.open,
    o.high,
    o.low,
    o.close,
    (o.high + o.low) / 2.0                      AS mid_range,
    o.volume,

    o.instrument_id
FROM option_ohlcv_1d_v o
JOIN equity_ohlcv_1d e
    ON e.ts_event::date = o.ts_event::date
   AND e.symbol = o.root_symbol;
