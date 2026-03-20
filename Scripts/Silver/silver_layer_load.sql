	-- loading into dw_silver.crm_cust_info

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'S' THEN 'Single'
				WHEN 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
			FROM bronze.crm_cust_info
			WHERE cst_id != 0
		) t
		WHERE flag = 1; -- Select the most recent record per customer




-- loading into dw_silver.crm_prd_info

TRUNCATE TABLE silver.crm_prd_info;
insert into dw_silver.crm_prd_info(
	prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
  )
  select 
	prd_id,
    substring(prd_key,7, length(prd_key)) as prd_key,
    replace(left(prd_key,5),'-', '_') as cat_id,
    prd_nm,
    prd_cost,
    case upper(trim(prd_line))
	when 'R' then 'Road'
    when 'S' then 'Other sales'
    when 'M' then 'Mountain'
    when 'T' then 'Touring'
    else 'N/A'
    end prd_line,
    prd_start_dt,
    lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - interval 1 day as prd_end_dt
  from crm_prd_info;

