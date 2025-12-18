/*
==============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Tranform and Load) process to populate
	the 'silver' schema tables from 'bronze' schema
	Actions Performed:
    - Truncate the silver tables before loading data
    - Inserts transformed and clean data from bronze tables to Silver tables
To Execute, Use :  EXEC Silver.load_silver
===============================================================================
*/
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	BEGIN TRY
		SET @start_time=GETDATE();
		PRINT '=========================================='
		PRINT 'LOADING SILVER LAYER';
		PRINT '=========================================='

		PRINT '------------------------------------------'
		PRINT 'LOADING CRM TABLES';
		PRINT '------------------------------------------'
		
		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Silver.crm_cust_info>>>'
		TRUNCATE TABLE Silver.crm_cust_info;
		PRINT '>>>Inserting Table Silver.crm_cust_info>>>'
		INSERT INTO Silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
		--CLEANING
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'Unknown'
			END cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'Unknown'
			END cst_gndr,
			cst_create_date
		FROM(
			SELECT 
				* ,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag
			FROM Bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)T WHERE Flag = 1 ;
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'


		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Silver.crm_prd_info>>>'
		TRUNCATE TABLE Silver.crm_prd_info;
		PRINT '>>>Inserting Table Silver.crm_prd_info>>>'
		INSERT INTO Silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		--cleaning
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'Unkown'
			END prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt         
		FROM Bronze.crm_prd_info
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

		
		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Silver.crm_sales_detail>>>'
		TRUNCATE TABLE Silver.crm_sales_detail;
		PRINT '>>>Inserting Table Silver.crm_sales_detail>>>'
		INSERT INTO Silver.crm_sales_detail(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		--cleaning
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
			END sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity *ABS(sls_price)
				 ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <=0 
					THEN sls_sales / NULLIF(sls_quantity,0)
				 ELSE sls_price
			END sls_price
		FROM Bronze.crm_sales_detail
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

		PRINT '------------------------------------------'
		PRINT 'LOADING ERP TABLES';
		PRINT '------------------------------------------'
		
		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Silver.erp_CUST_AZ12o>>>'
		TRUNCATE TABLE Silver.erp_CUST_AZ12;
		PRINT '>>>Inserting Table Silver.erp_CUST_AZ12>>>'
		INSERT INTO Silver.erp_CUST_AZ12(CID,BDATE,GEN)
		--cleaning
		SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE 
			END BDATE,
			CASE WHEN UPPER(TRIM(GEN)) = 'F' OR UPPER(TRIM(GEN)) = 'FEMALE' THEN 'Female'
				 WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
				ELSE 'Unknown'
			END GEN 
		FROM Bronze.erp_CUST_AZ12
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'


		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Silver.erp_LOC_A101>>>'
		TRUNCATE TABLE Silver.erp_LOC_A101;
		PRINT '>>>Inserting Table Silver.erp_LOC_A101>>>'
		INSERT INTO Silver.erp_LOC_A101(CID,CNTRY)
		--cleaning
		SELECT 
			REPLACE(CID,'-','') AS CID,
			CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
				 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'Unkown'
				 ELSE TRIM(CNTRY)
			END CNTRY
		FROM Bronze.erp_LOC_A101

		PRINT '>>>Truncating Table Silver.erp_PX_CAT_G1V2>>>'
		TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;
		PRINT '>>>Inserting Table Silver.erp_PX_CAT_G1V2>>>'
		INSERT INTO Silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
		--cleaning
		SELECT 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM Bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

		SET @end_time=GETDATE();
		PRINT '...Load Duration for Silver Layer: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Number'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END




