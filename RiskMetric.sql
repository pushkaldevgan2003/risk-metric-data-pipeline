create database risk_pipline;
use risk_pipline;
-- Asset master table
CREATE TABLE assets (
    asset_id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL,
    asset_name VARCHAR(100),
    asset_type VARCHAR(50)
);

-- Daily price data
CREATE TABLE daily_prices (
    price_id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES assets(asset_id),
    price_date DATE NOT NULL,
    close_price NUMERIC(12,4),
    UNIQUE(asset_id, price_date)
);

CREATE INDEX idx_prices_asset_date
ON daily_prices(asset_id, price_date);

-- Risk metrics table
CREATE TABLE risk_metrics (
    metric_id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES assets(asset_id),
    metric_date DATE,
    daily_return NUMERIC(10,6),
    rolling_volatility NUMERIC(10,6),
    var_95 NUMERIC(10,6),
    max_drawdown NUMERIC(10,6)
);

CREATE INDEX idx_risk_asset_date
ON risk_metrics(asset_id, metric_date);

select user, host from mysql.user where user ='root';
alter user 'root'@'localhost'
identified with mysql_native_password
by 'Devgan#2003';
flush privileges;
GRANT ALL PRIVILEGES ON risk_pipeline.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
SELECT a.ticker,
       r.metric_date,
       r.rolling_volatility,
       r.var_95,
       r.max_drawdown
FROM risk_metrics r
JOIN assets a ON r.asset_id = a.asset_id
ORDER BY r.metric_date DESC
LIMIT 10;
SELECT a.ticker,
       AVG(r.rolling_volatility) AS avg_volatility
FROM risk_metrics r
JOIN assets a ON r.asset_id = a.asset_id
GROUP BY a.ticker;
