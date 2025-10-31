/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouseprojecta' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'datawarehouseprojecta' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-------------------------------------------------------------------------------------------------------------------
-- Connect with master user
-- Drop and recreate the 'datawarehouseprojecta' database
-- Terminate all active connections to the target database (datawarehouseprojecta)
SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'datawarehouseprojecta' AND pid <> pg_backend_pid();

-- Drop the database if it exists
DROP DATABASE IF EXISTS "datawarehouseprojecta";

-- Create the 'DataWarehouse' database
CREATE DATABASE datawarehouseprojecta;

---------------------------------------------------------------------------------------------------------------------
--Connect to the new database and create schemas

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
