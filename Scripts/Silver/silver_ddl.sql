/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

USE dw_silver;

-- Creating tables for crm data
DROP TABLE IF EXISTS dw_silver.crm_cust_info;

CREATE TABLE dw_silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE,
    dw_create_date datetime default now()
);

DROP TABLE IF EXISTS dw_silver.crm_prd_info;

CREATE TABLE dw_silver.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
    dw_create_date datetime default now()
);

Drop TABLE IF EXISTS dw_silver.crm_sales_details;

CREATE TABLE dw_silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dw_create_date datetime default now()
);


-- Creating tables for erp data
Drop TABLE IF EXISTS dw_silver.erp_loc_info;
CREATE TABLE dw_silver.erp_loc_info (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
    dw_create_date datetime default now()
);

Drop TABLE IF EXISTS dw_silver.erp_cust_info;
CREATE TABLE dw_silver.erp_cust_info (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
    dw_create_date datetime default now()
);

Drop TABLE IF EXISTS dw_silver.erp_px_cat_info;

CREATE TABLE dw_silver.erp_px_cat_info (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
    dw_create_date datetime default now()
);
