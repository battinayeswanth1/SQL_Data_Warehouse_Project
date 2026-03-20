/*
=============================================================
Create Medallion Layer Databases (MySQL Version)
=============================================================
Script Purpose:
    This script sets up the environment for the Data Warehouse. 
    In MySQL, 'Schemas' are synonymous with 'Databases'. 
    We create three distinct databases to represent the Bronze, 
    Silver, and Gold layers.

WARNING:
    Running these 'DROP' commands will permanently delete 
    all data in these layers. Proceed with caution.
*/

-- 1. Drop existing databases to start fresh
DROP DATABASE IF EXISTS dw_bronze;
DROP DATABASE IF EXISTS dw_silver;
DROP DATABASE IF EXISTS dw_gold;

-- 2. Create the Bronze Layer (Raw data from source)
CREATE DATABASE dw_bronze 


-- 3. Create the Silver Layer (Cleansed & Standardized)
CREATE DATABASE dw_silver 


-- 4. Create the Gold Layer (Analytics / Star Schema)
CREATE DATABASE dw_gold 


-- 5. Verify the creation
SHOW DATABASES;
