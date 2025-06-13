-- 1. Bảng Category
CREATE TABLE Category (
    category_id     INT PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL
);

-- 2. Bảng Sub_Category
CREATE TABLE Sub_Category (
    sub_category_id    VARCHAR(50) PRIMARY KEY,
    sub_category_name  VARCHAR(100) NOT NULL,
    category_id        INT,
    FOREIGN KEY (category_id) REFERENCES Category(category_id)
);

-- 3. Bảng Product
CREATE TABLE Product (
    product_id       VARCHAR(50) PRIMARY KEY,
    product_name     VARCHAR(200) NOT NULL,
    price            FLOAT,
    sub_category_id  VARCHAR(50),
    FOREIGN KEY (sub_category_id) REFERENCES Sub_Category(sub_category_id)
);

-- 4. Bảng Customer
CREATE TABLE Customer (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    segment         VARCHAR(50),
    market          VARCHAR(50),
    region          VARCHAR(50),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100)
);

-- 5. Bảng Orders
CREATE TABLE Orders (
    order_id       VARCHAR(50) PRIMARY KEY,
    customer_id    VARCHAR(50),
    ship_city      VARCHAR(100),
    ship_state     VARCHAR(100),
    ship_country   VARCHAR(100),
    order_date     DATE,
    ship_date      DATE,
    ship_mode      VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

-- 6. Bảng Order_Details
CREATE TABLE Order_Details (
    order_id     VARCHAR(50),
    product_id   VARCHAR(50),
    quantity     INT,
    discount     FLOAT,
    sales        FLOAT,
    profit       FLOAT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Product(product_id)
);
