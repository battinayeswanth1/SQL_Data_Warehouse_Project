/*
===============================================================================
SILVER LAYER LOAD SCRIPT
===============================================================================
Purpose: 
    This script performs the ETL (Extract, Transform, Load) process to move data
    from the 'Bronze' (Raw) layer to the 'Silver' (Cleansed) layer.
    
Actions:
    - Truncates Silver tables before loading (Full Refresh).
    - Standardizes column formats and naming.
    - Cleanses 'dirty' data (Handling NULLs, fixing invalid dates).
    - Imputes missing values using logical calculations (Sales/Price).
    - Deduplicates records to ensure data integrity.
===============================================================================
*/

-- =============================================================================
-- 1. Loading crm_cust_info (Deduplicated & Standardized)
-- =============================================================================
TRUNCATE TABLE dw_silver.crm_cust_info;

INSERT INTO dw_silver.crm_cust_info (
    cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr, 
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    -- Normalize Marital Status
    CASE UPPER(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    -- Normalize Gender
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    -- Deduplication logic: Keep the most recent record per customer ID
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM dw_bronze.crm_cust_info
    WHERE cst_id != 0
) t
WHERE flag = 1;

-- =============================================================================
-- 2. Loading crm_prd_info (Key Extraction & Validity Ranges)
-- =============================================================================
TRUNCATE TABLE dw_silver.crm_prd_info;

INSERT INTO dw_silver.crm_prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    -- Extracting specific ID from composite key
    SUBSTRING(prd_key, 7) AS prd_key,
    REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
    prd_nm,
    prd_cost,
    -- Standardizing Product Line Categories
    CASE UPPER(TRIM(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    prd_start_dt,
    -- Calculating the end date by looking at the start date of the next record
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL 1 DAY
FROM dw_bronze.crm_prd_info;

-- =============================================================================
-- 3. Loading crm_sales_details (Data Imputation & Date Parsing)
-- =============================================================================
TRUNCATE TABLE dw_silver.crm_sales_details;

INSERT INTO dw_silver.crm_sales_details (
    sls_ord_num, 
    sls_prd_key, 
    sls_cst_id, 
    sls_order_dt, 
    sls_ship_dt, 
    sls_due_dt, 
    sls_sales, 
    sls_quantity, 
    sls_price
)
SELECT 
    sls_ord_num, 
    sls_prd_key, 
    sls_cst_id,
    -- Parsing integers into Date objects (YYYYMMDD)
    CASE WHEN sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
         ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d') END,
    CASE WHEN sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
         ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d') END,
    CASE WHEN sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
         ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d') END,
    -- Sales Imputation: Recalculate sales if value is zero or mathematically incorrect
    CASE 
        WHEN (sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)) 
             AND (sls_quantity > 0 AND sls_price > 0)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    -- Price Imputation: Derive price from sales if missing
    CASE
        WHEN (sls_price <= 0 OR sls_price IS NULL) AND sls_quantity > 0 
        THEN ROUND(sls_sales / sls_quantity)
        ELSE sls_price
    END
FROM dw_bronze.crm_sales_details;

-- =============================================================================
-- 4. Loading erp_cust_info (ID Standardization & Gender Normalization)
-- =============================================================================
TRUNCATE TABLE dw_silver.erp_cust_info;

INSERT INTO dw_silver.erp_cust_info (cst_cid, cst_bdate, cst_gen)
SELECT 
    -- Removing 'NAS' prefix to enable joins with CRM data
    CASE WHEN cst_cid LIKE 'NAS%' THEN SUBSTRING(cst_cid, 4)
         ELSE cst_cid END,
    cst_bdate, 
    CASE WHEN TRIM(UPPER(cst_gen)) IN ('M', 'MALE') THEN 'Male'
         WHEN TRIM(UPPER(cst_gen)) IN ('F', 'FEMALE') THEN 'Female'
         ELSE 'n/a' END
FROM dw_bronze.erp_cust_info;

-- =============================================================================
-- 5. Loading erp_loc_info (Country Standardization)
-- =============================================================================
TRUNCATE TABLE dw_silver.erp_loc_info;

INSERT INTO dw_silver.erp_loc_info (loc_cid, loc_cntry)
SELECT 
    REPLACE(loc_cid, '-', ''),
    -- Normalizing Country codes to full names for reporting
    CASE WHEN UPPER(TRIM(loc_cntry)) IN ('US', 'USA') THEN 'United States'
         WHEN UPPER(TRIM(loc_cntry)) = 'DE' THEN 'Germany'
         WHEN COALESCE(UPPER(TRIM(loc_cntry)), '') = '' THEN 'n/a'
         ELSE TRIM(loc_cntry)
    END
FROM dw_bronze.erp_loc_info;

-- =============================================================================
-- 6. Loading erp_px_cat_info (Category Lookup Standardization)
-- =============================================================================
TRUNCATE TABLE dw_silver.erp_px_cat_info;

INSERT INTO dw_silver.erp_px_cat_info (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    TRIM(cat),           -- Remove leading/trailing spaces for clean joins
    TRIM(subcat),        -- Standardize sub-category names
    TRIM(maintenance)    -- Ensure maintenance flags are clean
FROM dw_bronze.erp_px_cat_info;
