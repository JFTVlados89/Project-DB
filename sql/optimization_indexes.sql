-- Create only after the baseline EXPLAIN runs have been saved.
-- These indexes target fact-table join keys and supplier aggregation. PostgreSQL
-- may still choose a sequential scan when it is cheaper for a small/full-table
-- aggregate; compare actual plans and BUFFERS instead of assuming index scans.
CREATE INDEX IF NOT EXISTS idx_sales_record_product_sk
    ON public.sales_record USING btree (product_sk);

CREATE INDEX IF NOT EXISTS idx_sales_record_location_id
    ON public.sales_record USING btree (location_id);

CREATE INDEX IF NOT EXISTS idx_sales_record_age_gender
    ON public.sales_record USING btree (age_group_id, gender_id);

CREATE INDEX IF NOT EXISTS idx_sales_record_shipping_method_id
    ON public.sales_record USING btree (shipping_method_id);

CREATE INDEX IF NOT EXISTS idx_product_supplier_id
    ON public.product USING btree (supplier_id);

-- Refresh planner statistics after loading data and creating indexes.
ANALYZE public.sales_record;
ANALYZE public.product;
