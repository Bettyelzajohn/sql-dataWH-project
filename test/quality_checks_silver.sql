/*
Quality Checks for tables in silver layer.
====================================================================
checking for:
- null value
- unwanted space infront or trailing the words
- standard formats
- duplicates
- missing data
- unmeaningful data
*/

--silver.crm_cust_info
-- Checking for duplicates in primary key
SELECT 
	cst_id,
	COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
-- Checking for unwanted space
SELECT 
	cst_firstname --all the coloumns with string value
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT 
	cst_lastname --all the coloumns with string value
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- data standardization and consistency
SELECT DISTINCT(cst_gndr)
FROM Silver.crm_cust_info

SELECT DISTINCT(cst_marital_status)
FROM Silver.crm_cust_info

SELECT * FROM Silver.crm_cust_info



-- silver.crm_prd_info
-- Checking for duplicates in primary key
SELECT 
	prd_id,
	COUNT(*)
FROM Silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
-- Checking for unwanted space (we need no result to say its clean)
SELECT 
	prd_nm --all the coloumns with string value
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
-- checking for nulls and negative numbers
SELECT 
	prd_cost --all the coloumns with string value
FROM Silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
-- data standardization and consistency
SELECT DISTINCT(prd_line)
FROM Silver.crm_prd_info
-- checking for invalid date
SELECT *
FROM Silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM Silver.crm_prd_info



-- silver.crm_sales_detail
-- Checking for unwanted space (we need no result to say its clean)
SELECT 
	sls_ord_num --all the coloumns with string value
FROM Silver.crm_sales_detail
WHERE sls_ord_num != TRIM(sls_ord_num)
-- checking for invalid date ( Check all date columns)
SELECT 
NULLIF(sls_due_dt,0) AS sls_due_dt
FROM Silver.crm_sales_detail
WHERE sls_due_dt < 0 OR LEN(sls_due_dt) != 8 
                       OR sls_due_dt > 20500101
					   OR sls_due_dt < 19000101

SELECT *
FROM Silver.crm_sales_detail
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt                 
--checking data consistency between sales,quantity and price
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM Silver.crm_sales_detail
WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL 
                                            OR sls_quantity IS NULL
											OR sls_price IS NULL
											OR sls_sales <= 0 
                                            OR sls_quantity <= 0
											OR sls_price <= 0
ORDER BY sls_sales,sls_quantity,sls_price

SELECT * FROM Silver.crm_sales_detail









