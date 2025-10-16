/*
====================================================================================================
    Stored Procedure:   silver.load_silver
    Author:             Jonnalagadda Anand Reddy
    Creation Date:      2025-10-16

    Description:
    Performs a full refresh of the Silver layer tables from the Bronze layer.
    This procedure truncates each target table before loading cleansed and
    transformed data from its corresponding source table in the 'bronze' schema.

    Usage:
    EXEC silver.load_silver;
====================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    -- Suppress 'rows affected' messages for cleaner output and reduced network traffic
    SET NOCOUNT ON;

    -- Timing variables for logging
    DECLARE @batch_start_time DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @proc_start_time DATETIME2(3), @proc_end_time DATETIME2(3);

    BEGIN TRY
        PRINT '=================================================================';
        PRINT '[BEGIN] Silver Layer Load started at ' + CONVERT(NVARCHAR, @batch_start_time, 121);
        PRINT '=================================================================';

        ------------------------------------------------------------------------------------
        --  Load: silver.crm_cust_info
        --  Transformations: Deduplication, Cleansing, Normalization
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.crm_cust_info...';

        TRUNCATE TABLE silver.crm_cust_info;

        -- Use a CTE to identify the most recent record for each customer before inserting.
        -- This is a Data Structuring 🏗️ transformation.
        WITH LatestCustomerRecords AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname, -- Cleansing 🧼: Remove leading/trailing whitespace.
            TRIM(cst_lastname) AS cst_lastname,   -- Cleansing 🧼: Remove leading/trailing whitespace.
            -- Normalization 📏: Map single-letter codes to human-readable values.
            CASE UPPER(TRIM(cst_marital_status))
                WHEN 'S' THEN 'Single'
                WHEN 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            -- Normalization 📏: Map gender codes to human-readable values.
            CASE UPPER(TRIM(cst_gndr))
                WHEN 'F' THEN 'Female'
                WHEN 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM LatestCustomerRecords
        WHERE rn = 1; -- Structuring 🏗️: Filter to keep only the most recent record.

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        ------------------------------------------------------------------------------------
        --  Load: silver.crm_prd_info
        --  Transformations: Enrichment, Derivation, Normalization, Cleansing
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.crm_prd_info...';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Enrichment ➕: Extract a new feature (category ID) from the product key.
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Enrichment ➕: Extract the clean product key.
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,                       -- Cleansing 🧼: Handle NULL values by replacing them with 0.
            -- Normalization 📏: Map product line codes to descriptive values.
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            -- Derivation ➕: Calculate the record's end date (for versioning) as the day before the next version of the same product starts.
            CAST(LEAD(prd_start_dt, 1, '9999-12-31') OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) - 1 AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        ------------------------------------------------------------------------------------
        --  Load: silver.crm_sales_details
        --  Transformations: Cleansing (Type Casting), Derivation (Business Logic)
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.crm_sales_details...';

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- Cleansing 🧼: Safely convert integer YYYYMMDD to a valid DATE format. Returns NULL if conversion fails.
            TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8))) AS sls_order_dt,
            TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8))) AS sls_ship_dt,
            TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8))) AS sls_due_dt,
            -- Derivation ➕: Apply business logic to recalculate sales amount if it is invalid, missing, or inconsistent.
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            -- Derivation ➕: Apply business logic to derive the price if it is invalid or missing.
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        ------------------------------------------------------------------------------------
        --  Load: silver.erp_cust_az12
        --  Transformations: Cleansing, Validation, Normalization
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.erp_cust_az12...';

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            -- Cleansing 🧼: Remove 'NAS' prefix from customer ID.
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cid,
            -- Cleansing 🧼: Validate data by nullifying birth dates that are in the future.
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
            -- Normalization 📏: Standardize gender values from multiple formats ('F', 'FEMALE') to a single format.
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        ------------------------------------------------------------------------------------
        --  Load: silver.erp_loc_a101
        --  Transformations: Cleansing, Normalization
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.erp_loc_a101...';

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid, -- Cleansing 🧼: Remove hyphens for a consistent ID format.
            -- Normalization 📏: Map country codes to their full names and handle blank/null values.
            CASE UPPER(TRIM(cntry))
                WHEN 'DE' THEN 'Germany'
                WHEN 'US' THEN 'United States'
                WHEN 'USA' THEN 'United States'
                WHEN '' THEN 'n/a'
                ELSE ISNULL(TRIM(cntry), 'n/a')
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        ------------------------------------------------------------------------------------
        --  Load: silver.erp_px_cat_g1v2 (No transformations)
        ------------------------------------------------------------------------------------
        SET @proc_start_time = SYSUTCDATETIME();
        PRINT '[INFO] Loading table: silver.erp_px_cat_g1v2...';
        
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        -- This is a direct 1:1 load with no transformations applied.
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;

        SET @proc_end_time = SYSUTCDATETIME();
        PRINT '  -> Success. Duration: ' + CAST(DATEDIFF(MILLISECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' ms.';
        PRINT '-----------------------------------------------------------------';

        DECLARE @total_duration_seconds INT = DATEDIFF(SECOND, @batch_start_time, SYSUTCDATETIME());
        PRINT '=================================================================';
        PRINT '[SUCCESS] Silver Layer Load completed.';
        PRINT '    -> Total Duration: ' + CAST(@total_duration_seconds AS NVARCHAR) + ' seconds.';
        PRINT '=================================================================';

    END TRY
    BEGIN CATCH
        PRINT '=================================================================';
        PRINT '[FATAL] An error occurred during the Silver Layer load process!';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '=================================================================';

        -- Re-throw the error to ensure the calling process/job fails
        THROW;
    END CATCH;
END;
