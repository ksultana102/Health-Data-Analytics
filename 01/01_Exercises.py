##### INSTALL SQLAlchemy ##########################################################################
# %%
# If needed, run this and then restart the kernel. You will never have to run this again.
#%pip install --user SQLAlchemy psycopg2-binary

##### IMPORT / CONNECT / TEST #####################################################################
# %%
# SQLalchemy imports: create_engine and URL.
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Create a database engine.
url = URL.create(
    drivername = 'postgresql+psycopg2',
    host = 'localhost',
    port = '5432',
    database = 'cohe_6590',
    username = 'cohe_armc',
    password = 'cohe_armc_password'
)
engine = create_engine(url)
try:
    conn = engine.connect()
    conn.close()
    print('Connection successful')
except Exception as e:
    print('Unable to connect', e)

# Connect to the database and print the results from SELECT * FROM armc_1m.patients LIMIT 5.
with engine.connect() as con:
    rs = con.execute('SELECT * FROM armc_1m.patients LIMIT 5')
    for row in rs: print(row)

# Dispose of the engine.
engine.dispose()


##### SWITCH TO DATABASEUTILS PACKAGE #############################################################
# %%
# (1) Import databaseUtils as db and time
import sys
sys.path.append('../../python_utils/')
import databaseUtils as db 
import time

# %%
# Set each variable in the dictionary.
engineDetails = {
    'host'      : 'localhost',
    'port'      : 5432,
    'dbname'    : 'cohe_6590',
    'user'      : 'cohe_armc',
    'password'  : 'cohe_armc_password'
}


##### MIGRATE: SQL ################################################################################
# %%
# (1) Create a test schema if it does not exist (use db.execute(query, engineDetails) - manages 
#     engines for us).
db.execute('CREATE SCHEMA IF NOT EXISTS test', engineDetails)

# %%
# (2) Create table test.patients like armc_1m.patients and populate.
#     Fastest of all methods as the operations are performed directly on the database.
st = time.time()
db.execute('CREATE TABLE test.patients(LIKE arm_1m.patients INCLUDING ALL)', engineDetails)
db.execute('INSERT INTO test.patients SELECT * FROM armc_1m.patients ORDER BY patient_id', engineDetails)
print('Time:', time.time()-st)

# %%
# Print the DataFrame.head() of SELECT * FROM test.patients.
st = time.time()
print(db.executeQuery('SELECT * FROM test.patients', engineDetails).head())
print('Time:', time.time()-st)

# %%
# Print the results of SELECT * FROM test.patients LIMIT 5.
# Note: same output as above, but way faster!
st = time.time()
print(db.executeQuery('SELECT * FROM test.patients LIMIT 5', engineDetails))
print('Time:', time.time()-st)

# %%
# Drop test schema using an engine (use db.execute(query, engine)).
engine = db.engine(engineDetails)
db.execute('DROP SCHEMA test CASCADE', engine)
engine.dispose()


##### MIGRATE: PANDAS #############################################################################
# %%
# (1) Create an engine.
engine = ...

# %%
# (2) Create a test schema (same as before).
db.execute(..., engineDetails) 

# %%
# (3) Save SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000 (1m = forever!) as a 
#     DataFrame to a NEW table. Check pgAdmin. Notice how it wrote the DataFrame index! Control 
#     with to_sql index={True|False} - done later.
st = time.time()
df = db.executeQuery('SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000', engine)
df.to_sql(...)
print('Time:', time.time()-st)

# %%
# Drop and recreate test.patients like armc_1m.patients.
db.execute('DROP TABLE test.patients', engine)
db.execute('CREATE TABLE test.patients (LIKE armc_1m.patients INCLUDING ALL)', engine)

# %%
# (4) Save SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000 (1m = forever!) as a 
#     DataFrame to an EXISTING table (if_exists='append', index=False). Check pgAdmin. Notice how 
#     it did NOT write the DataFrame index!
st = time.time()
df = db.executeQuery('SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000', engine)
df.to_sql(...)
print('Time:', time.time()-st)


##### MIGRATE: PANDAS WITH COPY - 100K ############################################################
# %%
# Drop and recreate the test schema, and create test.patients like armc_1m.patients.
db.execute('DROP SCHEMA test CASCADE; CREATE SCHEMA test;', engine)
db.execute('CREATE TABLE test.patients (LIKE armc_1m.patients INCLUDING ALL)', engine)

# %%
# (1) Save SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000 (for comparison) as a
#     DataFrame to an EXISTING table using the db.psql_insert_copy method.
st = time.time()
df = db.executeQuery('SELECT * FROM armc_1m.patients ORDER BY patient_id LIMIT 100000', engine)
df.to_sql('patients', engine, schema='test', if_exists='append', index=False, method=...)
print('Time:', time.time()-st)


##### MIGRATE: PANDAS WITH COPY - 1M ##############################################################
# %%
# Drop and recreate the test schema, and create test.patients like armc_1m.patients.
db.execute('DROP SCHEMA test CASCADE; CREATE SCHEMA test;', engine)
db.execute('CREATE TABLE test.patients (LIKE armc_1m.patients INCLUDING ALL)', engine)

# %%
# Save all 1m using COPY
st = time.time()
df = db.executeQuery('SELECT * FROM armc_1m.patients ORDER BY patient_id', engine)
df.to_sql('patients', engine, schema='test', if_exists='append', index=False, method=db.psql_insert_copy)
print('Time:', time.time()-st)

# %%
# Drop schema test and dispose of engine.
db.execute('DROP SCHEMA test CASCADE', engine)
engine.dispose()