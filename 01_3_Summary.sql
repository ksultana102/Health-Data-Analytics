---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------
-- Create schema test (if it does not exist) and set search path to test
CREATE SCHEMA IF NOT EXISTS test;
SET SEARCH_PATH TO test;


---------------------------------------------------------------------------------------
-- Summary data: Encounters since 2010 
---------------------------------------------------------------------------------------
-- (1) Create encounters like armc_1k.encounters (include all) from 
--     armc_1k.encounters where admit year >= 2010
CREATE TABLE encounters (
	LIKE ...
);
INSERT INTO ...
SELECT ...

-- Output encounters
SELECT * FROM encounters;

-- (2) Create patients LIKE armc_1k.patients including only patients in encounters
--     The intent is to only load patients for which we have encounters
CREATE TABLE ...
INSERT INTO ...
SELECT ...

-- (3) Add the foreign key from encounters to patients
ALTER TABLE ...

-- Output atients
SELECT * FROM patients;


---------------------------------------------------------------------------------------
-- Drop cascade the test schema
---------------------------------------------------------------------------------------
-- Drop the test schema if it exists
DROP SCHEMA IF EXISTS test CASCADE;

