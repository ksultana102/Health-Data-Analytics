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
	LIKE armc_1k.encounters INCLUDING ALL
);
INSERT INTO encounters
SELECT * FROM armc_1k.encounters
WHERE EXTRACT(YEAR FROM admit_date) >= 2010
ORDER BY encounter_id;

-- Output encounters
SELECT * FROM encounters;

-- (2) Create patients LIKE armc_1k.patients including only patients in encounters
--     The intent is to only load patients for which we have encounters
CREATE TABLE patients (
	LIKE armc_1k.patients INCLUDING ALL
);
INSERT INTO patients
SELECT * FROM armc_1k.patients
WHERE patient_id IN (SELECT DISTINCT(patient_id) FROM encounters)
ORDER BY patient_id;

-- Output patients
SELECT * FROM patients;


---------------------------------------------------------------------------------------
-- Drop cascade the test schema
---------------------------------------------------------------------------------------
-- Drop the test schema if it exists
DROP SCHEMA IF EXISTS test CASCADE;

