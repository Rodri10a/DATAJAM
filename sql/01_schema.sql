-- ============================================
-- DataJam - Team ProductDetails
-- DDL Schema - MySQL 8.0
-- ============================================

CREATE DATABASE IF NOT EXISTS datajam;
USE datajam;

-- ============================================
-- 1. COUNTRIES (lookup table - no FK dependencies)
-- ============================================
CREATE TABLE countries (
    code        CHAR(2)         NOT NULL,
    name        VARCHAR(100)    NOT NULL,
    region      VARCHAR(50)     NOT NULL,
    population  INT UNSIGNED    NOT NULL,
    PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 2. CATEGORIES (lookup table - no FK dependencies)
-- ============================================
CREATE TABLE categories (
    id      INT             NOT NULL AUTO_INCREMENT,
    slug    VARCHAR(50)     NOT NULL,
    name    VARCHAR(100)    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_categories_slug (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 3. USERS (depends on: countries)
-- ============================================
CREATE TABLE users (
    id              INT             NOT NULL AUTO_INCREMENT,
    name            VARCHAR(150)    NOT NULL,
    email           VARCHAR(255)    NOT NULL,
    country_code    CHAR(2)         NOT NULL,
    created_at      DATE            NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_email (email),
    CONSTRAINT fk_users_country
        FOREIGN KEY (country_code) REFERENCES countries(code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 4. PRODUCTS (depends on: categories)
-- ============================================
CREATE TABLE products (
    id          INT             NOT NULL AUTO_INCREMENT,
    name        VARCHAR(255)    NOT NULL,
    price       DECIMAL(10,2)   NOT NULL,
    category_id INT             NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_products_category
        FOREIGN KEY (category_id) REFERENCES categories(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 5. PRODUCT_DETAILS (depends on: products)
-- ============================================
CREATE TABLE product_details (
    product_id  INT             NOT NULL,
    stock       INT             NOT NULL DEFAULT 0,
    rating      DECIMAL(3,2)    NOT NULL DEFAULT 0.00,
    weight      DECIMAL(8,2)    NOT NULL DEFAULT 0.00,
    PRIMARY KEY (product_id),
    CONSTRAINT fk_product_details_product
        FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 6. ORDERS (depends on: users)
-- ============================================
CREATE TABLE orders (
    id              INT             NOT NULL AUTO_INCREMENT,
    user_id         INT             NOT NULL,
    order_date      DATE            NOT NULL,
    total_amount    DECIMAL(12,2)   NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 7. ORDER_ITEMS (depends on: orders, products)
-- ============================================
CREATE TABLE order_items (
    id          INT             NOT NULL AUTO_INCREMENT,
    order_id    INT             NOT NULL,
    product_id  INT             NOT NULL,
    quantity    INT             NOT NULL,
    unit_price  DECIMAL(10,2)   NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders(id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 8. SHIPPING_REGIONS (depends on: countries)
-- ============================================
CREATE TABLE shipping_regions (
    country_code    CHAR(2)     NOT NULL,
    region          VARCHAR(50) NOT NULL,
    shipping_zone   VARCHAR(10) NOT NULL,
    estimated_days  INT         NOT NULL,
    PRIMARY KEY (country_code),
    CONSTRAINT fk_shipping_regions_country
        FOREIGN KEY (country_code) REFERENCES countries(code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

