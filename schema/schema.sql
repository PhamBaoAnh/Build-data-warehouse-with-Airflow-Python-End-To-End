-- Tạo schema warehouse nếu chưa tồn tại
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Tạo bảng dimension: dim_customer
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment TEXT
);

-- Tạo bảng dimension: dim_date
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    order_date DATE PRIMARY KEY,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    weekday TEXT,
    quarter INTEGER
);

-- Tạo bảng dimension: dim_location
CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_id VARCHAR PRIMARY KEY,
    city TEXT,
    state TEXT,
    country TEXT
);

-- Tạo bảng dimension: dim_product
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_id VARCHAR PRIMARY KEY,
    product_name TEXT,
    sub_category TEXT,
    category TEXT
);

-- Tạo bảng dimension: dim_shipmode
CREATE TABLE IF NOT EXISTS warehouse.dim_shipmode (
    shipmode_id VARCHAR PRIMARY KEY,
    ship_mode TEXT
);

-- Tạo bảng fact: fact_sales
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    order_id VARCHAR PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    location_id VARCHAR NOT NULL,
    shipmode_id VARCHAR NOT NULL,
    discount NUMERIC(5,2),
    profit NUMERIC(10,2),
    quantity INTEGER,
    sales NUMERIC(12,2),

    -- Khóa ngoại liên kết với bảng dimension
    FOREIGN KEY (order_date) REFERENCES warehouse.dim_date(order_date),
    FOREIGN KEY (customer_id) REFERENCES warehouse.dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES warehouse.dim_product(product_id),
    FOREIGN KEY (location_id) REFERENCES warehouse.dim_location(location_id),
    FOREIGN KEY (shipmode_id) REFERENCES warehouse.dim_shipmode(shipmode_id)
);
