-- Implied volatility computed from option_chain_1d using Black-Scholes.
-- NULL iv means no solution was found (deep ITM/OTM noise, DTE=0, or bad price).
CREATE TABLE IF NOT EXISTS option_iv_1d (
    trade_date  DATE             NOT NULL,
    root_symbol TEXT             NOT NULL,
    expiry      DATE             NOT NULL,
    option_type CHAR(1)          NOT NULL,
    strike      NUMERIC          NOT NULL,
    iv          DOUBLE PRECISION,
    PRIMARY KEY (trade_date, root_symbol, expiry, option_type, strike)
);
