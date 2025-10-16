/*
===============================================================================
Stored Procedure: load_bronze_layer
===============================================================================
Purpose:
    Loads data into the 'bronze' schema from external CSV files.

Actions:
    - Truncates all Bronze tables before loading new data.
    - Uses the COPY command to import CSV files into tables.
    - Logs progress and duration for each table load using RAISE NOTICE.

Usage:
    CALL load_bronze_layer();

Notes:
    - Ensure PostgreSQL has access permissions to the CSV file paths.
    - File paths below must match your local or server environment.
    - Run this procedure after all Bronze tables have been created.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE load_bronze_layer()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();

    RAISE NOTICE '================================================';
    RAISE NOTICE '       Starting Bronze Layer Load Process       ';
    RAISE NOTICE '================================================';

    --------------------------------------------------------------------------
    -- CRM TABLES
    --------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables...';
    RAISE NOTICE '------------------------------------------------'; 

    -- CRM Customer Info
    start_time := NOW();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/tmp/sql_data_warehouse_project/datasets/source_crm/cust_info.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded crm_cust_info in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- CRM Product Info
    start_time := NOW();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/tmp/sql_data_warehouse_project/datasets/source_crm/prd_info.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded crm_prd_info in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- CRM Sales Details
    start_time := NOW();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/tmp/sql_data_warehouse_project/datasets/source_crm/sales_details.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded crm_sales_details in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    --------------------------------------------------------------------------
    -- ERP TABLES
    --------------------------------------------------------------------------
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables...';
    RAISE NOTICE '------------------------------------------------';

    -- ERP Location
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/tmp/sql_data_warehouse_project/datasets/source_erp/loc_a101.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded erp_loc_a101 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- ERP Customer
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/tmp/sql_data_warehouse_project/datasets/source_erp/cust_az12.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded erp_cust_az12 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- ERP Product Category
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/tmp/sql_data_warehouse_project/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ',' CSV HEADER;
    end_time := NOW();
    RAISE NOTICE '>> Loaded erp_px_cat_g1v2 in % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    --------------------------------------------------------------------------
    -- COMPLETION
    --------------------------------------------------------------------------
    batch_end_time := NOW();

    RAISE NOTICE '================================================';
    RAISE NOTICE ' Bronze Layer Load Completed Successfully ';
    RAISE NOTICE ' Total Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE ' ERROR OCCURRED DURING BRONZE LAYER LOAD ';
        RAISE NOTICE ' SQLSTATE: %, MESSAGE: %', SQLSTATE, SQLERRM;
        RAISE NOTICE '=========================================='; 
END;
$$;

-- Run this manually to execute the load
CALL load_bronze_layer();
