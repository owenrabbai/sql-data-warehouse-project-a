/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    Loads data into the 'bronze' schema from external CSV files. 
    - Truncates bronze tables before loading data.
    - Uses COPY FROM to load CSV files from local paths.

Parameters:
    None.
Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
	DECLARE
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
		start_time TIMESTAMP;
		count_loaded BIGINT;
		end_time TIMESTAMP;
		duration NUMERIC;
		sql_query TEXT;
	BEGIN
		batch_start_time := clock_timestamp();
		RAISE NOTICE '================================================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '================================================';

		----------------------------------------------------------------------------
		-- Loading CRM Tables
		----------------------------------------------------------------------------
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '------------------------------------------------';

		-- crm_cust_info
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';

		sql_query := format(
			'COPY bronze.crm_cust_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_crm\cust_info.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.crm_cust_info;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		-- crm_prd_info
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';

		sql_query := format(
			'COPY bronze.crm_prd_info FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_crm\prd_info.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.crm_prd_info;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		-- crm_sales_details
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';

		sql_query := format(
			'COPY bronze.crm_sales_details FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_crm\sales_details.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.crm_sales_details;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		----------------------------------------------------------------------------
		-- Loading ERP Tables
		----------------------------------------------------------------------------
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '------------------------------------------------';

		-- erp_loc_a101
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';

		sql_query := format(
			'COPY bronze.erp_loc_a101 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_erp\LOC_A101.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.erp_loc_a101;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		-- erp_cust_az12
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';

		sql_query := format(
			'COPY bronze.erp_cust_az12 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_erp\cust_az12.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.erp_cust_az12;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		-- erp_px_cat_g1v2
		start_time := clock_timestamp();
		RAISE NOTICE '>> Start Time: %', start_time;
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

		sql_query := format(
			'COPY bronze.erp_px_cat_g1v2 FROM %L WITH (FORMAT CSV, HEADER TRUE, DELIMITER '','')',
			'C:\Users\Public\Project A\datasets\source_erp\px_cat_g1v2.csv'
		);
		EXECUTE sql_query;

		SELECT COUNT(*) INTO count_loaded FROM bronze.erp_px_cat_g1v2;
		end_time := clock_timestamp();
		duration := EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> Operation completed - Count loaded: %', count_loaded;
		RAISE NOTICE '>> Load Duration: % seconds', duration;
		RAISE NOTICE '';

		----------------------------------------------------------------------------
		-- Completion
		----------------------------------------------------------------------------
		batch_end_time := clock_timestamp();
		RAISE NOTICE '==========================================';
		RAISE NOTICE 'Loading Bronze Layer is Completed';
		RAISE NOTICE 'Batch Start Time: %', batch_start_time;
		RAISE NOTICE 'Batch Start Time: %', batch_end_time;
		RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
		RAISE NOTICE '==========================================';

		EXCEPTION
			WHEN OTHERS THEN
				RAISE NOTICE '==========================================';
				RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
				RAISE NOTICE 'Error Message: %', SQLERRM;
				RAISE NOTICE '==========================================';
	END;
$$;
