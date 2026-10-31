CREATE OR REPLACE VIEW v_api_product_catalog AS
SELECT id AS api_product_id, title, category, brand, sku, price, discount_percentage, rating, stock, source_system, ingested_at FROM api_products;
CREATE OR REPLACE VIEW v_api_product_price_distribution AS
SELECT category, COUNT(*) AS product_count, MIN(price) AS min_price,
 percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price, AVG(price) AS average_price, MAX(price) AS max_price
FROM api_products GROUP BY category;
CREATE OR REPLACE VIEW v_api_product_category_analysis AS
SELECT category, COUNT(*) AS product_count, AVG(price) AS average_price, AVG(rating) AS average_rating, SUM(stock) AS reported_stock
FROM api_products GROUP BY category;
CREATE OR REPLACE VIEW v_api_product_rating_analysis AS
SELECT id AS api_product_id, title, category, rating, price, stock FROM api_products WHERE rating IS NOT NULL;
CREATE OR REPLACE VIEW v_api_user_demographics AS
SELECT country, gender, COUNT(*) AS user_count, AVG(age)::numeric(6,2) AS average_age FROM api_users GROUP BY country, gender;
CREATE OR REPLACE VIEW v_api_cart_value_analysis AS
SELECT COUNT(*) AS cart_count, AVG(total) AS average_cart_total, AVG(discounted_total) AS average_discounted_total,
 SUM(total) AS sample_cart_total, AVG(total_quantity) AS average_item_quantity FROM api_carts;
CREATE OR REPLACE VIEW v_api_cart_product_analysis AS
SELECT (item->>'id')::bigint AS api_product_id, item->>'title' AS title,
 SUM((item->>'quantity')::integer) AS cart_quantity, COUNT(DISTINCT c.id) AS carts_containing_product,
 AVG((item->>'price')::numeric) AS average_listed_price
FROM api_carts c CROSS JOIN LATERAL jsonb_array_elements(c.products) AS item
GROUP BY (item->>'id')::bigint, item->>'title';
-- Exact SKU matches only. External sample records are not internal sales transactions.
CREATE OR REPLACE VIEW v_api_internal_product_match AS
SELECT ap.id AS api_product_id, ap.title AS api_title, ap.sku AS api_sku, p.product_id AS internal_product_id,
 p.product_name AS internal_title, p.sku AS internal_sku, ap.price AS api_price, p.price AS internal_price,
 ap.price-p.price AS price_difference
FROM api_products ap JOIN products p ON lower(trim(ap.sku))=lower(trim(p.sku))
WHERE NULLIF(trim(ap.sku),'') IS NOT NULL;
