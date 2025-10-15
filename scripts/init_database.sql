/*
=============================================================
Create Database and Schemas (PostgreSQL Version)
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 
    Then, it sets up three schemas: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will DROP the entire 'datawarehouse' database if it exists. 
    All data will be permanently deleted. Proceed with caution.
=============================================================
*/

-- Connect to the 'postgres' default database first
-- (You cannot drop the current database you're connected to)
\c postgres

-- Drop and recreate the 'datawarehouse' database
DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
        -- Terminate existing connections
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'datawarehouse';

        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;

    EXECUTE 'CREATE DATABASE datawarehouse';
END
$$;

-- Connect to the new database
\c datawarehouse

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
