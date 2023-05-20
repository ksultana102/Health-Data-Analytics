--DROP FUNCTION analyze_and_count_armc();
CREATE OR REPLACE FUNCTION analyze_and_count_armc() RETURNS TABLE ("Table" TEXT, "armc_1k" INT, "armc_10k" INT, "armc_100k" INT, "armc_1m" INT) AS $$
DECLARE
    schemas TEXT[] := array['armc_1k', 'armc_10k', 'armc_100k', 'armc_1m'];
    tables TEXT[] := array['departments', 'disease_types', 'diseases', 'encounters', 'medication_types', 'medications', 'patients', 'providers'];
    s TEXT;
    t TEXT;
	x TEXT;
	u TEXT := E'SELECT * FROM crosstab(''';
	y TEXT := E'\$\$VALUES';
	z TEXT := ') AS ct ("Table" TEXT, ';
	f BOOLEAN := true;
BEGIN
	FOREACH s IN ARRAY schemas LOOP
		IF f = false THEN 
			y := y || ', ';
			z := z || ', ';
		END IF;
		y := y || '(''' || s || '''::TEXT)';
		z := z || '"' || s || '" INT';
		FOREACH t IN ARRAY tables LOOP
			x := s || '.' || t;
			EXECUTE 'ANALYZE ' || x;
			--RAISE NOTICE '%', 'ANALYZE ' || x;
			IF f = false THEN 
				u := u || E'\n\tUNION ALL'; 
			ELSE
				f = false;
			END IF;
			u := u || E'\n\tSELECT ''''' || t || ''''' AS "Table", ''''' || s || ''''' AS "Schema", COUNT(*) AS ct FROM ' || x;
		END LOOP;
	END LOOP;
	u := u || E'\n\tORDER BY 1, 2''';
	u := u || E'\n\t, ' || y || E'\$\$\n';
	u := u || z || ')';
	EXECUTE 'CREATE EXTENSION IF NOT EXISTS tablefunc';
	RETURN QUERY EXECUTE u;
	--RAISE NOTICE '%', u;
END $$ LANGUAGE plpgsql;

SELECT * FROM analyze_and_count_armc();