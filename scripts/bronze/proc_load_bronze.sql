CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
DECLARE  @start_time DATETIME, @endtime DATETIME;
	BEGIN TRY
	PRINT '============================================';
	PRINT 'Loading Bronze layer';
	PRINT '============================================';


	PRINT '----------------------------------------------';
	PRINT 'Loading CRM Datae';
	PRINT '----------------------------------------------';

	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT  bronze.crm_cust_info
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);
	SET @endtime = GETDATE();
	PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';
	
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT  bronze.crm_prd_info
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);
	SET @endtime = GETDATE();
		PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';
	
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT  bronze.crm_sales_details
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);
	SET @endtime = GETDATE();
		PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';
	

	PRINT '----------------------------------------------';
	PRINT 'Loading ERP Datae';
	PRINT '----------------------------------------------';
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT  bronze.erp_cust_az12
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);

	SET @endtime = GETDATE();
		PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';
	
	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT  bronze.erp_loc_a101
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);

	SET @endtime = GETDATE();
	PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';

	SET @start_time = GETDATE();
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT  bronze.erp_px_cat_g1v2
	FROM 'C:\Users\user\Downloads\dataware\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH  (
		FIRSTROW = 2,
		FIELDTERMINATOR = ','
	
	);
	SET @endtime = GETDATE();
	PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';

	SET @endtime = GETDATE();
	END TRY
	BEGIN CATCH 
	PRINT '----------------------------------------------';
	PRINT 'Loading ERP Datae';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT  'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @endtime) AS NVARCHAR) + 'Seconds';
	PRINT '----------------------------------------------';
	END CATCH
	
END
