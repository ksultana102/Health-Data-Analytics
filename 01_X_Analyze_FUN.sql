DO $$
DECLARE
	schemas TEXT[] := array['armc_1k', 'armc_10k', 'armc_100k', 'armc_1m'];
	tables TEXT[] := array['departments', 'disease_types', 'diseases', 'encounters', 'medication_types', 'medications', 'patients', 'providers'];
	s TEXT;
	t TEXT;
BEGIN
	FOREACH s IN ARRAY schemas LOOP
		FOREACH t IN ARRAY tables LOOP
			EXECUTE 'ANALYZE ' || s || '.' || t;
		END LOOP;
	END LOOP;
END $$