---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Schema
---------------------------------------------------------------------------------------
-- (1) Create schema test (if it does not exist and set search path to armc_100k 
--     (try armc_1m outside of class).
--     Setting the search path to the source instead of the target makes sense if we
--     are writing a long of queries against the source.
CREATE SCHEMA IF NOT EXISTS test;
SET SEARCH_PATH TO armc_100k;


---------------------------------------------------------------------------------------
-- Denormalize patients and encounters into a single table
---------------------------------------------------------------------------------------
-- (1) Create table test.pat_enc_full
--     Use all patient attributes and all encounter attributes less patient_id
--     Separate admit_date and discharge_date into date, time, and timestamp attributes
CREATE TABLE test.pat_enc_full AS
SELECT p.*, e.encounter_id, e.admit_date::date AS admit_date, e.admit_date::time AS admit_time, e.admit_date AS admit_ts, 
		e.discharge_date::date AS discharge_date, e.discharge_date::time AS discharge_time, e.discharge_date AS discharge_ts, 
		e.department_id, e.provider_id, e.age, e.bp_systolic, e.bp_diastolic, e.temperature, e.pulse, e.height, e.weight
FROM patients p LEFT OUTER JOIN encounters e ON p.patient_id = e.patient_id
ORDER BY admit_date; -- essential for partitioning by date

-- (2) Query the new table for males with non-null ages seen during 2014 in descending date order
--     Check the Explain plan: seq scan/sort/gather merge on pat_enc_full
-- (2a) EXTRACT(YEAR FROM admit_date)
SELECT *
FROM test.pat_enc_full
WHERE gender = 'M'
AND age IS NOT NULL
AND EXTRACT(YEAR FROM admit_date) = 2014
ORDER BY admit_date DESC;

-- (2b) admit_date between...
--      While the query plan is the same, EXTRACT takes a little longer as it's an extra step
SELECT *
FROM test.pat_enc_full
WHERE gender = 'M'
AND admit_date BETWEEN '2014-01-01' AND '2014-12-31'
AND age IS NOT NULL
ORDER BY admit_date DESC;

-- (3) CREATE an index on admit_date (admit_date_full_idx)
--     Takes longer than the query, so it's only worth it if we use this multiple times (amortize)
--DROP INDEX test.admit_date_full_idx;
CREATE INDEX admit_date_full_idx ON test.pat_enc_full (admit_date);

-- (4) Rerun the test query
--     Check the Explain plan: index scan on admit_date_full_idx
-- (4a) Rerun (2a): Does NOT use the index (same time as above)
-- (4b) Rerun (2b): Uses the index



---------------------------------------------------------------------------------------
-- Denormalize patients and encounters into yearly partitioned tables
---------------------------------------------------------------------------------------
-- (1) Create table structure test.pat_enc_struct. Use all patient attributes and all encounter attributes less patient_id
--     Separate admit_date and discharge_date into date, time, and timestamp attributes
CREATE TABLE test.pat_enc_struct AS
SELECT p.*, e.encounter_id, e.admit_date::date AS admit_date, e.admit_date::time AS admit_time, e.admit_date AS admit_ts, 
		e.discharge_date::date AS discharge_date, e.discharge_date::time AS discharge_time, e.discharge_date AS discharge_ts, 
		e.department_id, e.provider_id, e.age, e.bp_systolic, e.bp_diastolic, e.temperature, e.pulse, e.height, e.weight
FROM patients p JOIN encounters e ON p.patient_id = e.patient_id
LIMIT 0; -- "trick": schema only

-- (2) Use test.pat_enc_struct to create test.pat_enc partitioned by range admit_date
CREATE TABLE test.pat_enc (LIKE test.pat_enc_struct INCLUDING ALL) PARTITION BY RANGE (admit_date);

-- Remove the struct table
DROP TABLE test.pat_enc_struct;

-- Yearly partitions: pgAdmin groups these under the root table (pat_enc)
DO $$
	DECLARE
		rec RECORD;
		q TEXT;
	BEGIN
		FOR rec IN SELECT DISTINCT(EXTRACT(YEAR FROM admit_date::date)) AS year FROM encounters ORDER BY 1 LOOP
			-- https://www.postgresql.org/docs/current/functions-string.html#FUNCTIONS-STRING-OTHER
			q := format(
				'CREATE TABLE IF NOT EXISTS %s PARTITION OF test.pat_enc FOR VALUES FROM (%L) TO (%L)', 
				'test.pat_enc_' || rec.year, rec.year || '-01-01', rec.year+1 || '-01-01'
			); -- for values from is [x, y)
			--RAISE NOTICE '%', q;
			EXECUTE q;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
/*
-- Daily
DO $$
	DECLARE
		rec RECORD;
		q TEXT;
	BEGIN
		FOR rec IN SELECT DISTINCT(admit_date::date) AS date FROM encounters ORDER BY 1 LOOP
			-- https://www.postgresql.org/docs/current/functions-string.html#FUNCTIONS-STRING-OTHER
			q := format(
				'CREATE TABLE IF NOT EXISTS %s PARTITION OF test.pat_enc FOR VALUES FROM (%L) TO (%L)', 
				'test.pat_enc_' || REPLACE(rec.date::text, '-', '_'), rec.date, (rec.date + INTERVAL '1 day')::date
			); -- for values from is [x, y)
			RAISE NOTICE '%', q;
			EXECUTE q;
		END LOOP;
	END;
$$ LANGUAGE plpgsql;
*/

-- (3) Insert records into the partitioned table (partitions are expensive to build)
INSERT INTO test.pat_enc
SELECT p.*, e.encounter_id, e.admit_date::date AS admit_date, e.admit_date::time AS admit_time, e.admit_date AS admit_ts, 
		e.discharge_date::date AS discharge_date, e.discharge_date::time AS discharge_time, e.discharge_date AS discharge_ts, 
		e.department_id, e.provider_id, e.age, e.bp_systolic, e.bp_diastolic, e.temperature, e.pulse, e.height, e.weight
FROM patients p LEFT OUTER JOIN encounters e ON p.patient_id = e.patient_id
ORDER BY admit_date; -- essential for partitioning by date

-- (4) Run the test query (change table to test.pat_enc)
--     Check the Explain plan: seq scan/sort/gather merge on pat_enc_full
--SET enable_partition_pruning = off;
--SET enable_partition_pruning = on;
SELECT *
FROM test.pat_enc
WHERE gender = 'M'
AND age IS NOT NULL
AND admit_date::date BETWEEN '2014-01-01' AND '2014-12-31'
ORDER BY admit_date::date DESC;

-- CREATE an index on admit_date (admit_date_idx)
--DROP INDEX test.admit_date_idx;
CREATE INDEX admit_date_idx ON test.pat_enc (admit_date);

-- (5) Rerun the test select query (4)
--     Check the Explain plan: index scan on pat_enc_2014_admit_date_idx


---------------------------------------------------------------------------------------
-- CTE example (if time)
---------------------------------------------------------------------------------------
-- (1) What is the percent of records in the 2014 partition? Use two CTEs.
WITH part AS (
	SELECT COUNT(*) AS cnt FROM test.pat_enc_2014
), total AS (
	SELECT COUNT(*) AS cnt FROM test.pat_enc
)
--SELECT *
--SELECT part.cnt/total.cnt -- 0??? Yes as it's integer division = floor
--SELECT part.cnt::numeric/total.cnt::numeric -- cast to numeric, but now it's ugly
SELECT ROUND((part.cnt::numeric/total.cnt::numeric)*100, 2)::text || '%' -- that's better!
FROM part, total -- cross join is fine since 1 x 1 = 1


---------------------------------------------------------------------------------------
-- Drop the test schema
---------------------------------------------------------------------------------------
-- Drop the test schema if it exists
DROP SCHEMA IF EXISTS test CASCADE;
