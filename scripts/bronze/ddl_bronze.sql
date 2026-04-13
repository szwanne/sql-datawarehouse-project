/*
===============================================================================
DDL Script: Create Bronze Tables & Load Data Into The Tables
===============================================================================
Script Purpose:
    The script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
    The script loads data into the newly created tables.
===============================================================================
*/

-- CUSTOMER INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_customers_dataset;
CREATE TABLE bronze_ecom.olist_customers_dataset(
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(50),
    customer_state VARCHAR(50)
);
SELECT * FROM bronze_ecom.olist_customers_dataset;

-- GEOLOCATION INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_geolocation_dataset;
CREATE TABLE bronze_ecom.olist_geolocation_dataset(
		geolocation_zip_code_prefix VARCHAR(10),
        geolocation_lat DECIMAL(10,8),
        geolocation_lng DECIMAL(10,8),
        geolocation_city VARCHAR(100),
        geolocation_state VARCHAR(2)
);

SELECT * FROM bronze_ecom.olist_geolocation_dataset;

-- ORDER ITEMS INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_order_items_dataset;
CREATE TABLE bronze_ecom.olist_order_items_dataset(
		order_id VARCHAR(50),
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        shipping_limit_date DATETIME,
        price INT,
        freight_value INT
);

SELECT * FROM bronze_ecom.olist_order_items_dataset;

-- ORDER PAYMENTS INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_order_payments_dataset;
CREATE TABLE bronze_ecom.olist_order_payments_dataset(
		order_id VARCHAR(50),
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value INT
);

SELECT * FROM bronze_ecom.olist_order_payments_dataset;

-- ORDER REVIEWS INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_order_reviews_dataset;
CREATE TABLE bronze_ecom.olist_order_reviews_dataset(
		review_id VARCHAR(50),
        order_id VARCHAR(50),
        review_score INT,
        review_comment_title VARCHAR(50),
        review_comment_message VARCHAR(300),
        review_creation_date DATETIME,
        review_answer_timestamp DATETIME
);

SELECT * FROM bronze_ecom.olist_order_reviews_dataset;

-- ORDERS INFROMATION
DROP TABLE IF EXISTS bronze_ecom.olist_orders_dataset;
CREATE TABLE bronze_ecom.olist_orders_dataset(
		order_id VARCHAR(50),
        customer_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp DATETIME,
        order_approved_at DATETIME,
        order_delivered_carrier_date DATETIME,
        order_delivered_customer_date DATETIME,
        order_estimated_delivery_date DATETIME
);

SELECT * FROM bronze_ecom.olist_orders_dataset;

-- PRODUCTS INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_products_dataset;
CREATE TABLE bronze_ecom.olist_products_dataset(
		product_id VARCHAR(50),
        product_category_name VARCHAR(100),
        product_name_lenght INT,
        product_description_lenght INT,
        product_photos_qty INT,
        product_weight_g INT,
        product_length_cm INT,
        product_height_cm INT,
        product_width_cm INT
);

SELECT * FROM bronze_ecom.olist_products_dataset;

-- SELLERS INFORMATION
DROP TABLE IF EXISTS bronze_ecom.olist_sellers_dataset;
CREATE TABLE bronze_ecom.olist_sellers_dataset(
		seller_id VARCHAR(50),
        seller_zip_code_prefix VARCHAR(10),
        seller_city VARCHAR(100),
        seller_state VARCHAR(50)
);

SELECT * FROM bronze_ecom.olist_sellers_dataset;

-- PRODUCT CATEGORY NAME TRANSLATION
DROP TABLE IF EXISTS bronze_ecom.olist_product_category_name_translation_dataset;
CREATE TABLE bronze_ecom.olist_product_category_name_translation_dataset(
		product_category_name VARCHAR(100),
        product_category_name_english VARCHAR(100)
);

SELECT * FROM bronze_ecom.olist_product_category_name_translation_dataset;

-- LOAD DATA

-- LOAD CUSTOMER INFORMATION
TRUNCATE TABLE bronze_ecom.olist_customers_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_customers_dataset.csv'
INTO TABLE bronze_ecom.olist_customers_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_customers_dataset;

-- LOAD GEOLOCATION INFORMATION
TRUNCATE TABLE bronze_ecom.olist_geolocation_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_geolocation_dataset.csv'
INTO TABLE bronze_ecom.olist_geolocation_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_geolocation_dataset;

-- LOAD ORDER ITEMS
TRUNCATE TABLE bronze_ecom.olist_order_items_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_order_items_dataset.csv'
INTO TABLE bronze_ecom.olist_order_items_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_order_items_dataset;

-- LOAD ORDER PAYMENTS
TRUNCATE TABLE bronze_ecom.olist_order_payments_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_order_payments_dataset.csv'
INTO TABLE bronze_ecom.olist_order_payments_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_order_payments_dataset;

-- LOAD ORDER REVIEWS
TRUNCATE TABLE bronze_ecom.olist_order_reviews_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_order_reviews_dataset.csv'
INTO TABLE bronze_ecom.olist_order_reviews_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_order_reviews_dataset;

-- LOAD ORDERS
TRUNCATE TABLE bronze_ecom.olist_orders_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_orders_dataset.csv'
INTO TABLE bronze_ecom.olist_orders_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_orders_dataset;

-- LOAD PRODUCTS
TRUNCATE TABLE bronze_ecom.olist_products_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_products_dataset.csv'
INTO TABLE bronze_ecom.olist_products_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_products_dataset;

-- LOAD SELLER 
TRUNCATE TABLE bronze_ecom.olist_sellers_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/olist_sellers_dataset.csv'
INTO TABLE bronze_ecom.olist_sellers_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_sellers_dataset;

-- LOAD PRODUCT CATEGORY NAME TRANSLATION
TRUNCATE TABLE bronze_ecom.olist_product_category_name_translation_dataset;
LOAD DATA LOCAL INFILE '/Users/siphozwane/Downloads/Data Engineering Project/Brazilian E-Commerce Public Dataset by Olist/product_category_name_translation.csv'
INTO TABLE bronze_ecom.olist_product_category_name_translation_dataset
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM bronze_ecom.olist_product_category_name_translation_dataset;