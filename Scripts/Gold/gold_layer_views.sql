/*
===============================================================================
GOLD LAYER VIEW CREATION
===============================================================================
Purpose: 
    This script creates the final business-ready views for the Gold Layer.
    It implements a Star Schema design consisting of Dimensions and Facts.
    
Architecture:
    - Dimensions: dimension_customer, dimension_product (SCD Type 1 logic)
    - Fact: fact_sales (Linking Dimensions and Measures)
===============================================================================
*/

-- =============================================================================
-- 1. Create Customer Dimension (Merging CRM & ERP Data)
-- =============================================================================
CREATE OR REPLACE VIEW dw_gold.dimension_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate key for Power BI
    ci.cst_id           AS customer_id,
    ci.cst_key          AS customer_number,
    ci.cst_firstname    AS first_name,
    ci.cst_lastname     AS last_name,
    la.loc_cntry        AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is primary source
        ELSE COALESCE(ca.cst_gen, 'n/a')           -- ERP is fallback
    END                 AS gender,
    ca.cst_bdate        AS birthdate,
    ci.cst_create_date  AS create_date
FROM dw_silver.crm_cust_info ci
LEFT JOIN dw_silver.erp_cust_info ca 
    ON ci.cst_key = ca.cst_cid -- Fixed alias and join key
LEFT JOIN dw_silver.erp_loc_info la 
    ON ci.cst_key = la.loc_cid; -- Fixed alias and join key


-- =============================================================================
-- 2. Create Product Dimension (Merging Product & Category info)
-- =============================================================================
CREATE OR REPLACE VIEW dw_gold.dimension_product AS
SELECT 	
    ROW_NUMBER() OVER (ORDER BY pd.prd_id) AS product_key, -- Surrogate key
    pd.prd_id           AS product_id, 
    pd.prd_key          AS product_number,
    pd.prd_nm           AS product_name,
    pd.cat_id           AS category_id,
    ct.cat              AS category,    -- Mapping from erp_px_cat_info
    ct.subcat           AS subcategory,
    ct.maintenance      AS maintenance,
    pd.prd_cost         AS product_cost,
    pd.prd_line         AS product_line, 
    pd.prd_start_dt     AS start_date
FROM dw_silver.crm_prd_info pd
LEFT JOIN dw_silver.erp_px_cat_info ct
    ON pd.cat_id = ct.id -- Standardized join
WHERE pd.prd_end_dt IS NULL; -- Filters only the current version of products


-- =============================================================================
-- 3. Create Fact Sales (The Central Fact Table)
-- =============================================================================
CREATE OR REPLACE VIEW dw_gold.fact_sales AS 
SELECT 
    cs.sls_ord_num      AS order_number,
    dp.product_key,      -- Linking to Product Dimension
    dc.customer_key,     -- Linking to Customer Dimension
    cs.sls_order_dt     AS order_date,
    cs.sls_ship_dt      AS shipping_date,
    cs.sls_due_dt       AS due_date,
    cs.sls_sales        AS sales_amount,
    cs.sls_quantity     AS quantity,
    cs.sls_price        AS price
FROM dw_silver.crm_sales_details cs
LEFT JOIN dw_gold.dimension_customer dc
    ON cs.sls_cst_id = dc.customer_id
LEFT JOIN dw_gold.dimension_product dp
    ON cs.sls_prd_key = dp.product_number;
