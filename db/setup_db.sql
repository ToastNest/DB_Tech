-- Existing
CREATE DATABASE sales_data;

CREATE TABLE orders (
    order_id TEXT,
    date TEXT,
    status TEXT,
    sales_channel TEXT,
    category TEXT,
    qty INT,
    amount FLOAT
);

-- New table for batch processing
CREATE TABLE batch_orders (
    order_id TEXT,
    category TEXT,
    amount FLOAT
);

-- New table for streaming processing
CREATE TABLE stream_orders (
    order_id TEXT,
    category TEXT,
    amount FLOAT
);
