---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------
-- Create schema covid
CREATE SCHEMA IF NOT EXISTS covid;

-- Extend privileges to on schema covid to cohe_armc (for Python)
GRANT ALL PRIVILEGES ON SCHEMA covid TO cohe_armc;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA covid TO cohe_armc;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA covid TO cohe_armc;
ALTER DEFAULT PRIVILEGES IN SCHEMA covid GRANT ALL PRIVILEGES ON TABLES TO cohe_armc;
ALTER DEFAULT PRIVILEGES IN SCHEMA covid GRANT ALL PRIVILEGES ON SEQUENCES TO cohe_armc;
/*
REVOKE ALL PRIVILEGES ON SCHEMA covid FROM cohe_armc;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA covid FROM cohe_armc;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA covid FROM cohe_armc;
ALTER DEFAULT PRIVILEGES IN SCHEMA covid REVOKE ALL PRIVILEGES ON TABLES FROM cohe_armc;
ALTER DEFAULT PRIVILEGES IN SCHEMA covid REVOKE ALL PRIVILEGES ON SEQUENCES FROM cohe_armc;
*/


---------------------------------------------------------------------------------------
-- Write all data to CSV
---------------------------------------------------------------------------------------
-- (1) Write armc_1k.patients to output/patients_all.csv
COPY ...


---------------------------------------------------------------------------------------
-- Write all data to TSV
---------------------------------------------------------------------------------------
-- (1) Write armc_1k.patients to output/patients_all.tsv
COPY ...


---------------------------------------------------------------------------------------
-- Write select attributes to CSV
---------------------------------------------------------------------------------------
-- (1) Write armc_1k.patients patient id gender and race to output/patients_select.csv
COPY ...

---------------------------------------------------------------------------------------
-- Write query results to TSV
---------------------------------------------------------------------------------------
-- (1) Write armc_1k.patients, females F only, to output/patients_limited.tsv
COPY ...
	

---------------------------------------------------------------------------------------
-- Read from CSV
---------------------------------------------------------------------------------------
-- (1) Create table covid.nyt_covid from NYT_COVID.csv's CSV structure (figure out from the file)
--     Open in Excel then in a text editor. Notice any differences (hint: date and fips)?
--     Include DROP TABLE IF EXISTS - working toward automation.
DROP TABLE IF EXISTS covid.nyt_covid;
CREATE TABLE covid.nyt_covid (
	...
);

-- Load the data using COPY.
COPY covid.nyt_covid FROM 
	'/Users/Shared/cohe_6590_exercises/data/NYT_COVID.csv'
	CSV HEADER;

-- Verify the load.
SELECT * FROM covid.nyt_covid;


---------------------------------------------------------------------------------------
-- Reading from TSV with a MM/DD/YYYY date format
---------------------------------------------------------------------------------------
-- (1) Create table covid.ncd_cli_ili from NCD_CLI_ILI.txt's TSV structure (figure out from the file)
--     Open in Excel then in a text editor. Notice any unusual formatting (hint: week end date)?
--     Include DROP TABLE IF EXISTS - working toward automation.
DROP TABLE IF EXISTS covid.ncd_cli_ili;
CREATE TABLE covid.ncd_cli_ili (
	...
);

-- (2) Change datestyle then load the data using COPY with a tab delimiter.
--     datestyle: https://www.postgresql.org/docs/12/datatype-datetime.html
SET ...
COPY covid.ncd_cli_ili FROM 
	'/Users/Shared/cohe_6590_exercises/data/NCD_CLI_ILI.txt'
	DELIMITER E'\t' CSV HEADER;
	
-- (3) Reset datestyle and verify the load.
RESET ...
SELECT * FROM covid.ncd_cli_ili;


---------------------------------------------------------------------------------------
-- Read JSON - FAIL
---------------------------------------------------------------------------------------
-- Create table covid.fhir for fhir_json/1.json...4.json - just data TEXT.
-- Include DROP TABLE IF EXISTS - working toward automation.
-- https://www.hl7.org/fhir/patient-examples.html
DROP TABLE IF EXISTS covid.fhir;
CREATE TABLE covid.fhir (
	data	TEXT
);

-- Read in fhir_json/1.json...4.json
-- encoding: https://www.postgresql.org/docs/9.3/multibyte.html
COPY covid.fhir FROM 
	'/Users/Shared/cohe_6590_exercises/data/fhir_json/1.json' ENCODING 'WIN1251';
COPY covid.fhir FROM 
	'/Users/Shared/cohe_6590_exercises/data/fhir_json/2.json' ENCODING 'WIN1251';
COPY covid.fhir FROM 
	'/Users/Shared/cohe_6590_exercises/data/fhir_json/3.json' ENCODING 'WIN1251';
COPY covid.fhir FROM 
	'/Users/Shared/cohe_6590_exercises/data/fhir_json/4.json' ENCODING 'WIN1251';
	
-- Pretty printed JSON does not play well with COPY. Need to convert first (Python).
SELECT * FROM covid.fhir;


---------------------------------------------------------------------------------------
-- Read JSON - PYTHON (remove pretty print) -> SQL = SUCCESS
---------------------------------------------------------------------------------------
-- Create table covid.fhir with fhir_id SERIAL, id TEXT, system TEXT, and data JSONB
-- Include DROP TABLE IF EXISTS - working toward automation.
-- https://www.hl7.org/fhir/patient-examples.html
DROP TABLE IF EXISTS covid.fhir;
CREATE TABLE covid.fhir (
	fhir_id	SERIAL,
	id		TEXT,
	system	TEXT,
	data	JSONB
);

-- Read in fhir_json_not_pp/1.json...4.json (no encoding)
COPY covid.fhir (data) FROM 
	'/Users/Shared/cohe_6590_exercises/data/output/fhir_json_not_pp/1.json';
COPY covid.fhir (data) FROM 
	'/Users/Shared/cohe_6590_exercises/data/output/fhir_json_not_pp/2.json';
COPY covid.fhir (data) FROM 
	'/Users/Shared/cohe_6590_exercises/data/output/fhir_json_not_pp/3.json';
COPY covid.fhir (data) FROM 
	'/Users/Shared/cohe_6590_exercises/data/output/fhir_json_not_pp/4.json';

-- Verify the load. Much better!
SELECT * FROM covid.fhir;

-- Select the idenfitier element using data -> 'identifier.' 
SELECT data -> 'identifier' FROM covid.fhir;

-- Oh joy, everything is an array (sarcasm)! Write a query to extract the array from the above
SELECT jsonb_array_elements(data -> 'identifier') FROM covid.fhir;

-- Using the above as the start to a CTE, extract the fhir_id, 'value', and 'system'
WITH x AS (
	SELECT fhir_id, jsonb_array_elements(data -> 'identifier') AS j FROM covid.fhir
)
SELECT fhir_id, j -> 'value', j -> 'system' from x;

-- Update id and system in covid.fhir with the value from the above
WITH x AS (
	SELECT fhir_id, jsonb_array_elements(data -> 'identifier') AS j FROM covid.fhir
)
UPDATE covid.fhir f SET id = j -> 'value', system = j -> 'system' FROM x WHERE f.fhir_id = x.fhir_id;

-- Verify the update.
SELECT * FROM covid.fhir;


---------------------------------------------------------------------------------------
-- Cleanup
---------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS covid CASCADE;