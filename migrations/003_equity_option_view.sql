CREATE OR REPLACE VIEW option_ohlcv_1d_v AS
SELECT
    ts_event,
    ts_event::date AS trade_date,
    symbol,
    trim(substr(symbol, 1, 6)) AS root_symbol,
    to_date(substr(symbol, 7, 6), 'YYMMDD') AS expiry,
    substr(symbol, 13, 1) AS option_type,
    substr(symbol, 14, 8)::numeric / 1000.0 AS strike,
    instrument_id,
    open,
    high,
    low,
    close,
    volume,
    to_date(substr(symbol, 7, 6), 'YYMMDD') - ts_event::date AS dte
FROM option_ohlcv_1d;

