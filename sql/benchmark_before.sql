-- Run this file after schema.sql and data load, before optimization_indexes.sql.
-- Run each query twice and keep the second execution in the comparable results
-- as the warm-cache run. Save the full EXPLAIN output for the report.
\timing on

-- Q1. Category demand and pricing: joins the largest fact table to product/category.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT c.category_name,
       COUNT(*) AS sales_rows,
       ROUND(AVG(p.price * (1 - s.discount / 100.0)), 2) AS avg_discounted_price
FROM public.sales_record AS s
JOIN public.product AS p ON p.product_sk = s.product_sk
JOIN public.category AS c ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY sales_rows DESC;

-- Q2. Geographic demand by category.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT l.country, l.city, c.category_name, COUNT(*) AS sales_rows
FROM public.sales_record AS s
JOIN public.location AS l ON l.location_id = s.location_id
JOIN public.product AS p ON p.product_sk = s.product_sk
JOIN public.category AS c ON c.category_id = p.category_id
GROUP BY l.country, l.city, c.category_name
ORDER BY sales_rows DESC
LIMIT 50;

-- Q3. Customer segment mix; demographics are aggregated and no direct identity exists.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT a.range_code, g.gender_name, COUNT(*) AS sales_rows,
       ROUND(AVG(s.discount), 2) AS avg_discount,
       ROUND(AVG(s.popularity_index), 2) AS avg_popularity
FROM public.sales_record AS s
JOIN public.age_group AS a ON a.age_group_id = s.age_group_id
JOIN public.gender AS g ON g.gender_id = s.gender_id
GROUP BY a.range_code, g.gender_name
ORDER BY sales_rows DESC;

-- Q4. Delivery cost and sales mix by shipping method.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT sm.method_name, COUNT(*) AS sales_rows,
       ROUND(AVG(s.shipping_cost), 2) AS avg_shipping_cost,
       ROUND(SUM(s.shipping_cost), 2) AS total_shipping_cost
FROM public.sales_record AS s
JOIN public.shipping_method AS sm ON sm.shipping_method_id = s.shipping_method_id
GROUP BY sm.method_name
ORDER BY total_shipping_cost DESC;

-- Q5. Supplier catalog and product return-rate summary.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)
SELECT p.supplier_id, COUNT(*) AS products,
       ROUND(AVG(p.return_rate), 2) AS avg_return_rate,
       SUM(p.stock_level) AS total_stock
FROM public.product AS p
GROUP BY p.supplier_id
ORDER BY avg_return_rate DESC NULLS LAST;
