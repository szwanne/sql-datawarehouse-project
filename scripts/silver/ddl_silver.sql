/*
===============================================================================
DDL Script: Create and Load Silver Tables -- (bronzer --> silver)
===============================================================================
Script Purpose:
    The script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

    The script also performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/


CREATE DATABASE silver_ecom;
-- CUSTOMER INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_customers_dataset;
CREATE TABLE silver_ecom.olist_customers_dataset(
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(50),
    customer_city VARCHAR(50),
    customer_state VARCHAR(50)
);

SELECT * FROM silver_ecom.olist_customers_dataset;

-- GEOLOCATION INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_geolocation_dataset;
CREATE TABLE silver_ecom.olist_geolocation_dataset(
		geolocation_zip_code_prefix VARCHAR(10),
        geolocation_lat DECIMAL(10,8),
        geolocation_lng DECIMAL(10,8),
        geolocation_city VARCHAR(100),
        geolocation_state VARCHAR(2)
);

SELECT * FROM silver_ecom.olist_geolocation_dataset;

-- ORDER ITEMS INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_order_items_dataset;
CREATE TABLE silver_ecom.olist_order_items_dataset(
		order_id VARCHAR(50),
        order_item_id INT,
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        shipping_limit_date DATETIME,
        price INT,
        freight_value INT
);

SELECT * FROM silver_ecom.olist_order_items_dataset;

-- ORDER PAYMENTS INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_order_payments_dataset;
CREATE TABLE silver_ecom.olist_order_payments_dataset(
		order_id VARCHAR(32),
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value INT
);

SELECT * FROM silver_ecom.olist_order_payments_dataset;

-- ORDER REVIEWS INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_order_reviews_dataset;
CREATE TABLE silver_ecom.olist_order_reviews_dataset(
		review_id VARCHAR(50),
        order_id VARCHAR(50),
        review_score INT,
        review_comment_title VARCHAR(50),
        review_comment_message VARCHAR(300),
        review_creation_date DATETIME,
        review_answer_timestamp DATETIME
);

SELECT * FROM silver_ecom.olist_order_reviews_dataset;

-- ORDERS INFROMATION
DROP TABLE IF EXISTS silver_ecom.olist_orders_dataset;
CREATE TABLE silver_ecom.olist_orders_dataset(
		order_id VARCHAR(50),
        customer_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp DATETIME,
        order_approved_at DATETIME,
        order_delivered_carrier_date DATETIME,
        order_delivered_customer_date DATETIME,
        order_estimated_delivery_date DATETIME
);

SELECT * FROM silver_ecom.olist_orders_dataset;

-- PRODUCTS INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_products_dataset;
CREATE TABLE silver_ecom.olist_products_dataset(
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

SELECT * FROM silver_ecom.olist_products_dataset;

-- SELLERS INFORMATION
DROP TABLE IF EXISTS silver_ecom.olist_sellers_dataset;
CREATE TABLE silver_ecom.olist_sellers_dataset(
		seller_id VARCHAR(50),
        seller_zip_code_prefix VARCHAR(10),
        seller_city VARCHAR(100),
        seller_state VARCHAR(50)
);

SELECT * FROM silver_ecom.olist_sellers_dataset;

-- PRODUCT CATEGORY NAME TRANSLATION
DROP TABLE IF EXISTS silver_ecom.olist_product_category_name_translation_dataset;
CREATE TABLE silver_ecom.olist_product_category_name_translation_dataset(
		product_category_name VARCHAR(100),
        product_category_name_english VARCHAR(100)
);

SELECT * FROM silver_ecom.olist_product_category_name_translation_dataset;

-- CLEANING DATA
-- CUSTOMER INFORMATION

-- Goal: Remove inverted commas in customer_id, customer_unique and zip code column

UPDATE bronze_ecom.olist_customers_dataset
SET customer_id = REPLACE(customer_id, '"', '');

UPDATE bronze_ecom.olist_customers_dataset
SET customer_unique_id = REPLACE(customer_unique_id, '"', '');

UPDATE bronze_ecom.olist_customers_dataset
SET customer_zip_code_prefix = REPLACE(customer_zip_code_prefix, '"', '');


-- Check for that all the customer_id consists of 32 digits
SELECT COUNT(*)
from (SELECT customer_id from bronze_ecom.olist_customers_dataset
GROUP BY customer_id
Having count(customer_id) != 32)
as counting;

-- check for unwanted spaces
--  expectation: no result
SELECT customer_city
FROM bronze_ecom.olist_customers_dataset
WHERE customer_city != TRIM(customer_city);


-- Capitilize the first letter of customer city
UPDATE bronze_ecom.olist_customers_dataset
SET customer_city = CONCAT(
    UPPER(LEFT(customer_city, 1)),
    LOWER(SUBSTRING(customer_city, 2))
);

-- Check for duplicates

SELECT 
customer_id,
count(*)
FROM bronze_ecom.olist_customers_dataset
GROUP BY customer_id
HAVING count(*) > 1;

SELECT * FROM bronze_ecom.olist_customers_dataset;

INSERT INTO silver_ecom.olist_customers_dataset (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM bronze_ecom.olist_customers_dataset;

SELECT * FROM silver_ecom.olist_customers_dataset;

-- GEOLOCATION INFORMATION 

-- REMOVING INVERTED COMMAS
UPDATE bronze_ecom.olist_geolocation_dataset
SET geolocation_zip_code_prefix = REPLACE(geolocation_zip_code_prefix, '"','');

-- CAPITALIZING THE FIRST LETTER IN GEO CITY
UPDATE bronze_ecom.olist_geolocation_dataset
SET geolocation_city = CONCAT(
    UPPER(LEFT(geolocation_city, 1)),
    LOWER(SUBSTRING(geolocation_city, 2))
);

SELECT * FROM bronze_ecom.olist_geolocation_dataset;

INSERT INTO silver_ecom.olist_geolocation_dataset (
	geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
SELECT 
	geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM bronze_ecom.olist_geolocation_dataset;

SELECT * FROM silver_ecom.olist_geolocation_dataset;

-- ORDER ITEMS INFORMATION

-- REMOVE THE DUPLICATES
UPDATE bronze_ecom.olist_order_items_dataset
SET order_id = REPLACE(order_id, '"','');

UPDATE bronze_ecom.olist_order_items_dataset
SET product_id = REPLACE(product_id, '"','');

UPDATE bronze_ecom.olist_order_items_dataset
SET seller_id = REPLACE(seller_id, '"','');

-- CHECK FOR NULL DATES OR '0000-00-00 OR PRICE BEING ZERO'
SELECT * FROM bronze_ecom.olist_order_items_dataset
WHERE price = 0;

SELECT * FROM bronze_ecom.olist_order_items_dataset;


INSERT INTO silver_ecom.olist_order_items_dataset(
	order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT
	order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM bronze_ecom.olist_order_items_dataset;

SELECT * FROM silver_ecom.olist_order_items_dataset;


-- ORDER PAYMENTS INFORMATION

-- REMOVE INVERTED COMMAS

UPDATE bronze_ecom.olist_order_payments_dataset
SET order_id = REPLACE(order_id, '"','');

SELECT * FROM bronze_ecom.olist_order_payments_dataset;

INSERT INTO silver_ecom.olist_order_payments_dataset(
	order_id,
    payment_sequential,
    payment_type, 
    payment_installments,
    payment_value
)
SELECT
	order_id,
    payment_sequential,
    payment_type, 
    payment_installments,
    payment_value
FROM bronze_ecom.olist_order_payments_dataset;

SELECT * FROM silver_ecom.olist_order_payments_dataset;


-- ORDER REVIEWS INFORMATION

-- REMOVE THE INVERTED COMMAS

UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_id = REPLACE(review_id, '"','');

UPDATE bronze_ecom.olist_order_reviews_dataset
SET order_id = REPLACE(order_id, '"','');

-- SELECT * FROM bronze_ecom.olist_order_reviews_dataset
-- where review_id = '';

-- REMOVES THE BAD REVIEW_ID DATA THAT DOESN'T CONFORM TO HASH SERIAL CODE
SELECT review_id, COUNT(*)
FROM bronze_ecom.olist_order_reviews_dataset
GROUP BY review_id
HAVING review_id = '';

SELECT *
FROM bronze_ecom.olist_order_reviews_dataset
WHERE review_id NOT REGEXP '^[a-f0-9]{32}$';

DELETE FROM bronze_ecom.olist_order_reviews_dataset
WHERE review_id NOT REGEXP '^[a-f0-9]{32}$';


-- ADDING NULLS TO REVIEW COMMENTS WHERE ITS EMPTY OR NUMBERS
UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_comment_title = NULL
WHERE review_comment_title IS NULL
   OR review_comment_title = ''
   OR review_comment_title REGEXP '^[0-9]+$';

UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_comment_message = REPLACE(review_comment_message, '"','');

UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_comment_message = NULL
WHERE review_comment_message IS NULL
	OR review_comment_message = ''
    OR review_comment_message REGEXP '^[0-9]+$*';


UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_creation_date = NULL
WHERE review_creation_date = 0;

UPDATE bronze_ecom.olist_order_reviews_dataset
SET review_answer_timestamp = NULL
WHERE review_answer_timestamp = 0;

-- SELECT
--     CASE 
--         WHEN review_creation_date = 0
--         THEN NULL
--         ELSE review_creation_date
--     END AS review_creation_date
-- FROM bronze_ecom.olist_order_reviews_dataset;

-- SELECT review_creation_date
-- FROM bronze_ecom.olist_order_reviews_dataset
-- WHERE review_creation_date = 0
--    OR review_creation_date IS NULL;

-- REVIEW CREATION DATE

SELECT * FROM bronze_ecom.olist_order_reviews_dataset;


INSERT INTO silver_ecom.olist_order_reviews_dataset(
	review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
    )
SELECT
	review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM bronze_ecom.olist_order_reviews_dataset;

SELECT * FROM silver_ecom.olist_order_reviews_dataset;

-- ORDER INFORMATION

-- REMOVING THE INVERTED COMMAS
UPDATE bronze_ecom.olist_orders_dataset
SET order_id = REPLACE(order_id, '"', '');

UPDATE bronze_ecom.olist_orders_dataset
SET customer_id = REPLACE(customer_id, '"', '');

-- ADDING THE NULLS IN DATES THAT ARE 0

UPDATE bronze_ecom.olist_orders_dataset
SET order_approved_at = NULL
WHERE order_approved_at = 0;

UPDATE bronze_ecom.olist_orders_dataset
SET order_delivered_carrier_date = NULL
WHERE order_delivered_carrier_date = 0;

UPDATE bronze_ecom.olist_orders_dataset
SET order_delivered_customer_date = NULL
WHERE order_delivered_customer_date = 0;

UPDATE bronze_ecom.olist_orders_dataset
SET order_estimated_delivery_date = NULL
WHERE order_estimated_delivery_date = 0;

SELECT * FROM bronze_ecom.olist_orders_dataset
where order_estimated_delivery_date = 0;

SELECT * FROM bronze_ecom.olist_orders_dataset;

INSERT INTO silver_ecom.olist_orders_dataset(
	order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT 
	order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
FROM bronze_ecom.olist_orders_dataset;

SELECT * FROM silver_ecom.olist_orders_dataset;

-- PRODUCT INFORMATION

-- REMOVE COMMAS, EMPTY PRODUCT NAMES

UPDATE bronze_ecom.olist_products_dataset
SET product_id = REPLACE(product_id, '"','');

DELETE FROM bronze_ecom.olist_products_dataset
WHERE product_category_name = '';

SELECT * FROM bronze_ecom.olist_products_dataset;

INSERT INTO silver_ecom.olist_products_dataset(
	product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT 
	product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM bronze_ecom.olist_products_dataset;

SELECT * FROM silver_ecom.olist_products_dataset;

-- SELLERS INFORMATION

-- REMOVE INVERTED COMMAS

UPDATE bronze_ecom.olist_sellers_dataset
SET seller_id = REPLACE(seller_id, '"', '');

UPDATE bronze_ecom.olist_sellers_dataset
SET seller_zip_code_prefix = REPLACE(seller_zip_code_prefix, '"', '');

-- CAPITALIZING THE FIRST LETTER OF THE SELLER CITY

UPDATE bronze_ecom.olist_sellers_dataset
SET seller_city = CONCAT(
    UPPER(LEFT(seller_city, 1)),
    LOWER(SUBSTRING(seller_city, 2))
);

SELECT * FROM bronze_ecom.olist_sellers_dataset;

INSERT INTO silver_ecom.olist_sellers_dataset(
	seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT 
	seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM bronze_ecom.olist_sellers_dataset;

SELECT * FROM silver_ecom.olist_sellers_dataset;

-- PRODUCT CATEGORY NAME TRANSLATION

SELECT * FROM bronze_ecom.olist_product_category_name_translation_dataset;

-- REMOVING UNDERSCORES

UPDATE bronze_ecom.olist_product_category_name_translation_dataset
SET product_category_name = REPLACE(product_category_name, '_', ' ');

UPDATE bronze_ecom.olist_product_category_name_translation_dataset
SET product_category_name_english = REPLACE(product_category_name_english, '_', ' ');


-- CAPITALIZING THE FIRST LETTER OF THE SELLER CITY

UPDATE bronze_ecom.olist_product_category_name_translation_dataset
SET product_category_name = CONCAT(
    UPPER(LEFT(product_category_name, 1)),
    LOWER(SUBSTRING(product_category_name, 2))
);

UPDATE bronze_ecom.olist_product_category_name_translation_dataset
SET product_category_name_english = CONCAT(
    UPPER(LEFT(product_category_name_english, 1)),
    LOWER(SUBSTRING(product_category_name_english, 2))
);

SELECT * FROM bronze_ecom.olist_product_category_name_translation_dataset;

INSERT INTO silver_ecom.olist_product_category_name_translation_dataset(
	product_category_name,
    product_category_name_english
    )
SELECT
	product_category_name,
    product_category_name_english
FROM bronze_ecom.olist_product_category_name_translation_dataset;

SELECT * FROM silver_ecom.olist_product_category_name_translation_dataset;
