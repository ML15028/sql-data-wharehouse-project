CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time as DATETIME, @end_time as DATETIME
BEGIN TRY

    PRINT '==============================================='
	PRINT 'loading bronze layer'
	PRINT '==============================================='
	PRINT 'TRUNCATE AND INSERT TABLE bronze.crm_cust_info'
	PRINT '==============================================='
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	PRINT '==============================================='
	PRINT 'TRUNCATE AND INSERT TABLE bronze.crm_prd_info'
	PRINT '==============================================='
	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

PRINT '==============================================='
PRINT 'TRUNCATE AND INSERT TABLE bronze.crm_sales_details'
PRINT '==============================================='
	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);


	PRINT '==============================================='
	PRINT 'TRUNCATE AND INSERT TABLE bronze.erp_cust_az12'
	PRINT '==============================================='
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT '==============================================='
	PRINT 'TRUNCATE AND INSERT TABLE bronze.erp_loc_a101'
	PRINT '==============================================='

	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	PRINT '==============================================='
	PRINT 'TRUNCATE AND INSERT TABLE  bronze.erp_px_cat_g1v2'
	PRINT '==============================================='
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\Bey\Documents\Franklin\Data warehouse project\source data\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST (DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) + ' seconds';
END TRY
BEGIN CATCH
	PRINT '==============================================='
	PRINT 'ERROR OCCURED WHILE LOADING THE BRONZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT '==============================================='
END CATCH
END
