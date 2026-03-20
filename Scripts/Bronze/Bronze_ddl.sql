/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE dw_bronze;

-- Creating tables for crm data
DROP TABLE IF EXISTS dw_bronze.crm_cust_info;

CREATE TABLE dw_bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
);

DROP TABLE IF EXISTS dw_bronze.crm_prd_info;

CREATE TABLE dw_bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

Drop TABLE IF EXISTS dw_bronze.crm_sales_details;

CREATE TABLE dw_bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


-- Creating tables for erp data
Drop TABLE IF EXISTS dw_bronze.erp_loc_info;
CREATE TABLE dw_bronze.erp_loc_info (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

Drop TABLE IF EXISTS dw_bronze.erp_cust_info;
CREATE TABLE dw_bronze.erp_cust_info (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

Drop TABLE IF EXISTS dw_bronze.erp_px_cat_info;

CREATE TABLE dw_bronze.erp_px_cat_info (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
