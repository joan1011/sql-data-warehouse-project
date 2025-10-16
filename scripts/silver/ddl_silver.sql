/*
====================================================================================================
    DDL Script:         Create Silver Layer Tables
    Author:             [Anand Reddy Jonnalagadda]
    Creation Date:      2025-10-16

    Description:
    This script defines the table structures for the 'silver' schema. It ensures that
    any existing tables are dropped before creation to guarantee a clean setup.
    The Silver layer contains cleansed, standardized, and integrated data.

    Run this script to set up or reset the DDL for the Silver layer.
====================================================================================================
*/

-- Create the schema if it does not already exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

--------------------------------------------------------------------------------
-- Table: silver.crm_cust_info
-- Purpose: Stores cleansed and deduplicated customer master data.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT PRIMARY KEY,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- Table: silver.crm_prd_info
-- Purpose: Stores cleansed product information with calculated versioning dates.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id             INT PRIMARY KEY,
    cat_id             NVARCHAR(50),
    prd_key            NVARCHAR(50),
    prd_nm             NVARCHAR(100),
    prd_cost           DECIMAL(18, 4),
    prd_line           NVARCHAR(50),
    prd_start_dt       DATE,
    prd_end_dt         DATE,
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- Table: silver.crm_sales_details
-- Purpose: Stores cleansed and validated sales transaction data.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num        NVARCHAR(50),
    sls_prd_key        NVARCHAR(50),
    sls_cust_id        INT,
    sls_order_dt       DATE,
    sls_ship_dt        DATE,
    sls_due_dt         DATE,
    sls_sales          DECIMAL(18, 4),
    sls_quantity       INT,
    sls_price          DECIMAL(18, 4),
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME(),
    -- A composite key is common for sales detail tables
    PRIMARY KEY (sls_ord_num, sls_prd_key)
);
GO

--------------------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-- Purpose: Stores cleansed customer location information from the ERP system.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid                NVARCHAR(50) PRIMARY KEY,
    cntry              NVARCHAR(100),
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-- Purpose: Stores additional cleansed customer attributes from the ERP system.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid                NVARCHAR(50) PRIMARY KEY,
    bdate              DATE,
    gen                NVARCHAR(50),
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);
GO

--------------------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-- Purpose: Stores product category and subcategory master data from the ERP.
--------------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id                 NVARCHAR(50) PRIMARY KEY,
    cat                NVARCHAR(50),
    subcat             NVARCHAR(50),
    maintenance        NVARCHAR(50),
    dwh_inserted_at    DATETIME2(3) DEFAULT SYSUTCDATETIME()
);
GO
