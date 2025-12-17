/*
==============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external csv files.
    It performs :
    - Truncate the bronze tables before loading data
    -Uses the 'BULK INSERT' command  to load data from the csv files to bronze tables
To Execute, Use :  EXEC Bronze.load_bronze
===============================================================================
*/
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	BEGIN TRY
		SET @start_time=GETDATE();
		PRINT '=========================================='
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=========================================='

		PRINT '------------------------------------------'
		PRINT 'LOADING CRM TABLES';
		PRINT '------------------------------------------'

		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.crm_cust_info>>>'
		TRUNCATE TABLE Bronze.crm_cust_info;
		PRINT '>>>Inserting Table Bronze.crm_cust_info>>>'
		BULK INSERT Bronze.crm_cust_info
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'


		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.crm_prd_info>>>'
		TRUNCATE TABLE Bronze.crm_prd_info;
		PRINT '>>>Inserting Table Bronze.crm_prd_info>>>'
		BULK INSERT Bronze.crm_prd_info
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'


		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.crm_sales_detail>>>'
		TRUNCATE TABLE Bronze.crm_sales_detail;
		PRINT '>>>Inserting Table Bronze.crm_sales_detail>>>'
		BULK INSERT Bronze.crm_sales_detail
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

		PRINT '------------------------------------------'
		PRINT 'LOADING ERP TABLES';
		PRINT '------------------------------------------'

		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.erp_CUST_AZ12>>>'
		TRUNCATE TABLE Bronze.erp_CUST_AZ12;
		PRINT '>>>Inserting Table Bronze.erp_CUST_AZ12>>>'
		BULK INSERT Bronze.erp_CUST_AZ12
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

	
		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.erp_LOC_A101>>>'
		TRUNCATE TABLE Bronze.erp_LOC_A101;
		PRINT '>>>Inserting Table Bronze.erp_LOC_A101>>>'
		BULK INSERT Bronze.erp_LOC_A101
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'


		SET @start_time=GETDATE();
		PRINT '>>>Truncating Table Bronze.erp_PX_CAT_G1V2>>>'
		TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2;
		PRINT '>>>Inserting Table Bronze.erp_PX_CAT_G1V2>>>'
		BULK INSERT Bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Betty.ElzaJohn\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '...Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
		PRINT'***********************************'

		SET @end_time=GETDATE();
		PRINT '...Load Duration for Bronze Layer: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'sec';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Number'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END


