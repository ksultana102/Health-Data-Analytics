---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------
-- (1) Create schema test (if it does not exist) and set search path
CREATE SCHEMA IF NOT EXISTS test;
SET SEARCH_PATH TO test;


---------------------------------------------------------------------------------------
-- RAW: Create table then populate
---------------------------------------------------------------------------------------
-- (1) Create table patients (patient_id INTEGER PRIMARY KEY) from armc_1k.patients
CREATE TABLE patients (patient_id INTEGER PRIMARY KEY);
INSERT INTO patients
SELECT patient_id FROM armc_1k.patients ORDER BY patient_id;

-- (2) Output patients
SELECT * FROM patients;


---------------------------------------------------------------------------------------
-- RAW: Create table from query results then add constraints
---------------------------------------------------------------------------------------
-- (1) Drop patients if exists
DROP TABLE IF EXISTS patients;

-- (2) Create patients AS armc_1k.patients (order it)
CREATE TABLE patients AS
SELECT * FROM armc_1k.patients ORDER BY patient_id;

-- (3) Add a primary key on patients (patient_id)
ALTER TABLE patients ADD CONSTRAINT patients_pk PRIMARY KEY (patient_id);

-- Output patients
SELECT * FROM patients;


---------------------------------------------------------------------------------------
-- RAW: Create table like then populate
---------------------------------------------------------------------------------------
-- (1) Drop patients if exists
DROP TABLE IF EXISTS patients;

-- (2) Create patients like armc_1k.patients (include all) from armc_1k.patients
CREATE TABLE patients (
	LIKE armc_1k.patients INCLUDING ALL
);
INSERT INTO patients
SELECT * FROM armc_1k.patients ORDER BY patient_id;

-- Output patients
SELECT * FROM patients;


---------------------------------------------------------------------------------------
-- Drop the test schema
---------------------------------------------------------------------------------------
-- (1) Drop the test schema if it exists
DROP SCHEMA IF EXISTS test CASCADE;