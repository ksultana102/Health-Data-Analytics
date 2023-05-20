##### INITIALIZE ##################################################################################
# %%
# Imports
import sys
sys.path.append('../../python_utils/')
import pandas as pd
import databaseUtils as db
import time

# Set each variable in the dictionary.
engineDetails = {
    'host'      : 'localhost',
    'port'      : 5432,
    'dbname'    : 'cohe_6590',
    'user'      : 'cohe_armc',
    'password'  : 'cohe_armc_password'
}

# Directories
dataDir = '../../data/'
outputDir = dataDir+'output/'


##### WRITE: CSV ##################################################################################
# %%
# (1) Write SELECT * FROM armc_1k.patients to patients_pd.csv
rs = db.executeQuery('SELECT * FROM armc_1k.patients', engineDetails)
rs.to_csv(outputDir+'patients_pd.csv', index=False, header=True)


##### WRITE: TSV ##################################################################################
# %%
# (1) Write the same results (reuse) to patients_pd.tsv
rs.to_csv(outputDir+'patients_pd.tsv', index=False, header=True, sep='\t')


##### READ: CSV ###################################################################################
# %%
# (1) Read in NYT_COVID.csv using pd.read_csv. Print first 10 using head.
covid = pd.read_csv(dataDir+'NYT_COVID.csv')
print(covid.head(10))

# %% 
# Print covid.info(). What do you notice?
#   date: object instead of datetime64[ns]
#   fips: float64 instead of string with leading zeros
#   deaths: float64 instead of int64
print(covid.info())

# %%
# (2) Save to new table covid.nyt_covid_pd using db.psql_insert_copy. Ignore the index and fail if exists. 
st = time.time()
covid.to_sql('nyt_covid_pd', db.engine(engineDetails), schema='covid', if_exists='fail', index=False, method=db.psql_insert_copy)
print('Time:', time.time()-st)


##### READ: TSV ###################################################################################
# %%
# (1) Read in NCD_CLI_ILI.txt using pd.read_csv and a tab delimiter. Print first 10 using head.
ncd = pd.read_csv(dataDir+'NCD_CLI_ILI.txt', delimiter='\t')
print(ncd.head(10))

# %% 
# Print covid.info(). What do you notice?
# week: object instead of datetime64[ns]
print(ncd.info())


##### READ: SAS XPT ###############################################################################
# %%
# Read in DEMO_J.XPT using pd.read_sas. Print first 10 using head.
demo = pd.read_sas(dataDir+'DEMO_J.XPT')
print(demo.head(10))
print(demo.info())

# %%
# Save it as covid.demo_j.
# Go to PostgreSQL and look at the data. What happened???
st = time.time()
demo.to_sql('demo_j', db.engine(engineDetails), schema='covid', if_exists='fail', index=False, method=db.psql_insert_copy)
print('Time:', time.time()-st)

# %%
# Quick correction. Convert certain columns to int. We probably won't work with SAS files again, 
# so I wanted to share the cleaning script now.
import numpy as np
demo_j_ints = [[True]*39+[False]*2+[True]*4+[False]*1]  # int = true, float = false
temp_value = -987987987                                 # a number guaranteed not in the set
zero_error = 5.397605346934028e-78                      # 5.397605346934028e-79 -> 78, approx number so slight adjust or it won't work
i = 0
# for each column (these are column-level transformations)
for col in demo:
    # if convert from float to int (i.e., demo_j_ints[i] == True), convert
    if(demo_j_ints[0][i]):
        '''
        (1) fillna with the temp value to allow for int casting (cannot have NaN or inf values when casting)
        (2) astype(int) casts to an int
        (3) now that the type is int, np.NaN is allowed - replace temp value with np.NaN -> float64
        (4) yes, the data type is back to float64, but it will now allow casting to a nullable int pd.Int64Dtype() - crazy!
        '''
        demo[col] = demo[col] \
                    .fillna(temp_value) \
                    .astype(int) \
                    .replace(temp_value, np.NaN) \
                    .astype(pd.Int64Dtype())
                    
    else:
        '''
        (1) 5.397605346934028e-79 is present in the floats as well, replace with 0
        '''
        demo[col] = demo[col].apply(lambda x : x if x > zero_error else 0)
    i += 1
        
print(demo.info())

# %%
# Save it as covid.demo_j.
# Go back to PostgreSQL and check out the table
st = time.time()
db.execute('DROP TABLE IF EXISTS covid.demo_j', engineDetails)
demo.to_sql('demo_j', db.engine(engineDetails), schema='covid', if_exists='fail', index=False, method=db.psql_insert_copy)
print('Time:', time.time()-st)


##### READ/WRITE: JSON - COMBINED #################################################################
# %%
import json
import os
dir_in = dataDir+'fhir_json/'
dir_out = outputDir+'fhir_json_not_pp/'
entries = os.listdir(dir_in)
for entry in entries:
    # read pretty printed JSON
    with open(dir_in+entry) as f:
        data = json.load(f)
    
    # write PostgreSQL-compliant non-pretty printed JSON
    with open(dir_out+entry, 'w') as f:
        s = str(json.dumps(data))       # dump to json as string
        s = s.replace(r'\"', r'\\"')    # double-escape \" -> \\" or Postgres will error
        s = s.replace(r'\n', r'\\n')    # 0x0a = \n -> \\n or Postgres will error
        s = s.replace(r'\t', r'\\t')    # 0x09 = \t -> \\t or Postgres will error
        f.write(s)


