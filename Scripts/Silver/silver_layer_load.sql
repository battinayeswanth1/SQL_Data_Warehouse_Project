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


truncate table dw_silver.crm_sales_details;
insert into dw_silver.crm_sales_details(
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
select 
	sls_ord_num, 
    sls_prd_key, 
    sls_cst_id,
	case
		when sls_order_dt <= 0 or length(sls_order_dt) != 8 then null
        else str_to_date(sls_order_dt , '%Y%m%d') 
        end sls_order_dt,
	case
		when sls_ship_dt <= 0 or length(sls_ship_dt) != 8 then null
        else str_to_date(sls_ship_dt , '%Y%m%d') 
        end sls_ship_dt,
	case
		when sls_due_dt <= 0 or length(sls_due_dt) != 8 then null
        else str_to_date(sls_due_dt , '%Y%m%d') 
        end sls_due_dt,
	case 
		when (sls_sales <=0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price)) and (sls_quantity >0 and sls_price >0)
        then sls_quantity * abs(sls_price)
        else sls_sales
        end sls_sales,
	sls_quantity,
	case
		when sls_price <=0 then round(sls_sales/sls_quantity)
        else sls_price
        end sls_price
from crm_sales_details


truncate table dw_silver.erp_cust_info;
insert into dw_silver.erp_cust_info(
	cst_cid, 
    cst_bdate, 
    cst_gen
)
select 
	case
		when cst_cid like 'NAS%' then substring(cst_cid,4,length(cst_cid))
        else cst_cid
        end cst_cid,
    cst_bdate, 
	case trim(upper(cst_gen))
		when 'M' then 'Male'
		when 'F' then 'Female'
		when '' then null
		else trim(cst_gen)
		end cst_gen
 from erp_cust_info;



