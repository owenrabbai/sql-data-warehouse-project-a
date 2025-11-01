/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    Transforms and standardizes data from the Bronze schema and loads it into
    the Silver schema. 
    - Cleans, normalizes, and enriches data using business rules.
    - Ensures data consistency and integrity.
    - Applies transformations for gender, marital status, product line, etc.

Parameters:
    None.
Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration NUMERIC;
    count_loaded BIGINT;
BEGIN
    batch_start_time := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer (Bronze -> Silver)';
    RAISE NOTICE '================================================';

    ----------------------------------------------------------------------------
    -- Loading CRM Tables
    ----------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Transforming and Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- =====================================================
    -- crm_cust_info
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';

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
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) IN ('M','MALE') THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) IN ('F','FEMALE') THEN 'Female'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date 
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1 AND cst_id IS NOT NULL;

    SELECT COUNT(*) INTO count_loaded FROM silver.crm_cust_info;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    -- =====================================================
    -- crm_prd_info
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        prd_cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1,5),'-','_') AS prd_cat_id,
        SUBSTRING(prd_key, 7,LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost,0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) 
             - INTERVAL '1 day' AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;

    SELECT COUNT(*) INTO count_loaded FROM silver.crm_prd_info;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    -- =====================================================
    -- crm_sales_details
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt <= 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt <= 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt <= 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    SELECT COUNT(*) INTO count_loaded FROM silver.crm_sales_details;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    ----------------------------------------------------------------------------
    -- Loading ERP Tables
    ----------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Transforming and Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- =====================================================
    -- erp_cust_az12
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
             ELSE cid
        END AS cid,
        CASE WHEN bdate > clock_timestamp() THEN NULL
             ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    SELECT COUNT(*) INTO count_loaded FROM silver.erp_cust_az12;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    -- =====================================================
    -- erp_loc_a101
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid,'-','') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    SELECT COUNT(*) INTO count_loaded FROM silver.erp_loc_a101;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    -- =====================================================
    -- erp_px_cat_g1v2
    -- =====================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Start Time: %', start_time;
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    SELECT COUNT(*) INTO count_loaded FROM silver.erp_px_cat_g1v2;
    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Operation Completed - Count Loaded: %', count_loaded;
    RAISE NOTICE '>> Load Duration: % seconds', duration;
    RAISE NOTICE '';

    ----------------------------------------------------------------------------
    -- Completion
    ----------------------------------------------------------------------------
    batch_end_time := clock_timestamp();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Silver Layer Loading Completed Successfully';
    RAISE NOTICE 'Batch Start Time: %', batch_start_time;
    RAISE NOTICE 'Batch End Time: %', batch_end_time;
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;
