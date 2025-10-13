CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE NOT NULL,
    name   VARCHAR(255),
    exchange VARCHAR(20)
);
INSERT INTO symbols (id, symbol, name, exchange)
VALUES 
    (1, 'AAPL', 'Apple Inc', 'NASDAQ'),
    (2, 'MSFT', 'Microsoft', 'NASDAQ');
