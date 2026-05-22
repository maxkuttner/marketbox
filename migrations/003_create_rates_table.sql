-- Daily 3-month Treasury bill rate (DTB3) from FRED.
-- DTB3 = "3-Month Treasury Bill Secondary Market Rate": the annualized yield on
-- 3-month US T-bills, used as the risk-free rate in Black-Scholes pricing.
-- Stored as annualized percent (e.g. 5.25 means 5.25%); divide by 100 for BS.
-- Weekends and holidays have no entry — FRED reports no rate those days.
CREATE TABLE IF NOT EXISTS rates_1d (
    trade_date  DATE             PRIMARY KEY,
    dtb3        DOUBLE PRECISION NOT NULL
);
