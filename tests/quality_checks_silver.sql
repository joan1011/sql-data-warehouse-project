/*
==========================================================================================
    Data Quality & Validation Test Suite: Silver Layer
==========================================================================================
    Author:             [Your Name]
    Date:               2025-10-16

    Purpose:
    This script executes a suite of data quality checks on the 'silver' data layer
    to validate integrity, consistency, and accuracy after an ETL load.

    How to Use:
    Execute this script and review the results for each test. Any rows returned by a
    query indicate a data quality issue that requires investigation.
==========================================================================================
*/
SET NOCOUNT ON;

PRINT '=======================================================';
PRINT '🚀 Executing Silver Layer Data Quality Validation Suite...';
PRINT '=======================================================';


------------------------------------------------------------------------------------------
-- 1. Table: silver.crm_cust_info
------------------------------------------------------------------------------------------
PRINT CHAR(10) + '--- 1. Testing Table: silver.crm_cust_info ---';

-- Test CUST-01: Primary Key Integrity (No NULLs)
-- Finding:     The primary key 'cst_id' should not contain any NULL values.
-- Action:      If rows are returned, investigate the source data or transformation logic.
PRINT '  -> Running CUST-01: Primary Key Integrity (No NULLs)';
SELECT cst_id FROM silver.crm_cust_info WITH (NOLOCK) WHERE cst_id IS NULL;

-- Test CUST-02: Primary Key Integrity (No Duplicates)
-- Finding:     Each 'cst_id' must be unique.
-- Action:      If rows are returned, investigate the deduplication logic in the ETL.
PRINT '  -> Running CUST-02: Primary Key Integrity (No Duplicates)';
SELECT cst_id, COUNT(*) AS DuplicateCount
FROM silver.crm_cust_info WITH (NOLOCK)
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Test CUST-03: Standardization Check (Marital Status)
-- Finding:     The 'cst_marital_status' field should only contain predefined values.
-- Action:      If rows are returned, update the ETL transformation to handle new or incorrect values.
PRINT '  -> Running CUST-03: Standardization Check (Marital Status)';
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info WITH (NOLOCK)
WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a');


------------------------------------------------------------------------------------------
-- 2. Table: silver.crm_prd_info
------------------------------------------------------------------------------------------
PRINT CHAR(10) + '--- 2. Testing Table: silver.crm_prd_info ---';

-- Test PROD-01: Primary Key Integrity (No NULLs or Duplicates)
-- Finding:     The primary key 'prd_id' must be unique and not NULL.
-- Action:      Investigate source data or ETL logic.
PRINT '  -> Running PROD-01: Primary Key Integrity';
SELECT prd_id, COUNT(*) AS RecordCount
FROM silver.crm_prd_info WITH (NOLOCK)
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Test PROD-02: Data Validation (Product Cost)
-- Finding:     Product cost must be a non-negative number.
-- Action:      If rows are returned, check the source data for invalid cost entries.
PRINT '  -> Running PROD-02: Data Validation (Product Cost)';
SELECT prd_id, prd_cost
FROM silver.crm_prd_info WITH (NOLOCK)
WHERE prd_cost < 0;

-- Test PROD-03: Logical Consistency (Date Range)
-- Finding:     The start date 'prd_start_dt' must be before the end date 'prd_end_dt'.
-- Action:      Review the SCD (Slowly Changing Dimension) logic in the ETL process.
PRINT '  -> Running PROD-03: Logical Consistency (Date Range)';
SELECT prd_id, prd_start_dt, prd_end_dt
FROM silver.crm_prd_info WITH (NOLOCK)
WHERE prd_start_dt > prd_end_dt;


------------------------------------------------------------------------------------------
-- 3. Table: silver.crm_sales_details
------------------------------------------------------------------------------------------
PRINT CHAR(10) + '--- 3. Testing Table: silver.crm_sales_details ---';

-- Test SALE-01: Logical Consistency (Date Sequence)
-- Finding:     The order date cannot be after the shipping or due date.
-- Action:      Investigate date fields in the source transaction system.
PRINT '  -> Running SALE-01: Logical Consistency (Date Sequence)';
SELECT sls_ord_num, sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver.crm_sales_details WITH (NOLOCK)
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Test SALE-02: Financial Integrity (Sales Calculation)
-- Finding:     The 'sls_sales' amount must equal 'sls_quantity * sls_price'.
-- Action:      Review the calculation logic in the ETL and the source data's accuracy.
PRINT '  -> Running SALE-02: Financial Integrity (Sales Calculation)';
SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details WITH (NOLOCK)
WHERE ABS(sls_sales - (sls_quantity * sls_price)) > 0.01; -- Use a small tolerance for decimal precision


------------------------------------------------------------------------------------------
-- 4. Table: silver.erp_cust_az12
------------------------------------------------------------------------------------------
PRINT CHAR(10) + '--- 4. Testing Table: silver.erp_cust_az12 ---';

-- Test ERPCUST-01: Data Validation (Birthdate Range)
-- Finding:     Birthdates must be within a reasonable range (e.g., not in the future or too far in the past).
-- Action:      Correct invalid birthdates in the source ERP system.
PRINT '  -> Running ERPCUST-01: Data Validation (Birthdate Range)';
SELECT cid, bdate
FROM silver.erp_cust_az12 WITH (NOLOCK)
WHERE bdate > GETDATE() OR bdate < '1920-01-01';

-- Test ERPCUST-02: Standardization Check (Gender)
-- Finding:     The 'gen' field should only contain predefined values.
-- Action:      Update the ETL to handle any non-standard gender values.
PRINT '  -> Running ERPCUST-02: Standardization Check (Gender)';
SELECT DISTINCT gen
FROM silver.erp_cust_az12 WITH (NOLOCK)
WHERE gen NOT IN ('Female', 'Male', 'n/a');


------------------------------------------------------------------------------------------
-- 5. Table: silver.erp_loc_a101
------------------------------------------------------------------------------------------
PRINT CHAR(10) + '--- 5. Testing Table: silver.erp_loc_a101 ---';

-- Test ERPLOC-01: Standardization Check (Country)
-- Finding:     The 'cntry' field should only contain cleansed, full country names.
-- Action:      Review the country mapping in the ETL process.
PRINT '  -> Running ERPLOC-01: Standardization Check (Country)';
SELECT DISTINCT cntry
FROM silver.erp_loc_a101 WITH (NOLOCK)
WHERE cntry NOT IN ('Germany', 'United States', 'n/a'); -- Add other valid countries as needed


PRINT CHAR(10) + '=======================================================';
PRINT '✅ Validation Suite Execution Complete.';
PRINT '=======================================================';
