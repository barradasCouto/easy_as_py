# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 00:16:30 2020

@author: gcouto
"""

import os
import snowflake.connector
import pandas as pd

# Define job to run. Make Sure that a .txt file is saved to sqlCodeDir, with 
# code exactly as you would see it in Snowflake. Else just will not run.
timeDesc = 'FY20Q4W01_'
jobName = timeDesc + 'sfTableDesc'
boxDir = os.path.join('C:\\', 'Users',os.environ['USERNAME'],
                                 'Box', 'GBC','DLJ', 'Data Analytics Office')
inputDir = os.path.join('C:\\', 'Users', os.environ['USERNAME'])
sqlCodeDir = os.path.join(boxDir, 'Snowflake', 'Snowflake_codes')
jobFolderDir = os.path.join(sqlCodeDir, jobName)
mkJobFolder = os.mkdir(jobFolderDir)
outputDir = os.path.join(boxDir, 'Raw Data')

df = pd.read_csv(os.path.join(outputDir, 'FY20Q4W01_SF_TABLE_LIST' + '.csv'))

for ind in df.index: 
    codeString = 'DESCRIBE TABLE "' + str(df['database_name'][ind]) + '"."' + str(df['schema_name'][ind]) + '"."' + str(df['name'][ind]) + '"'
    jobFile = str(df['name'][ind]) + '.txt'
    jobFileDir = os.path.join(jobFolderDir, jobFile)
    with open(jobFileDir, "w+") as f:
        f.write(codeString)
    
# with open(os.path.join(sqlCodeDir, jobName + '.txt'), 'r') as file:
#     sqlCode = file.read()

# # Connect to Snowflake using SAML 2.0-compliant IdP federated authentication
# con = snowflake.connector.connect(
# user='{}@cisco.com'.format(os.environ['USERNAME']),
#     account='cisco.us-east-1', 
#     authenticator='externalbrowser',
#     role='DSX_DLJCOE_BUS_ANALYST_ROLE',
#     warehouse='DSX_DLJCOE_RPT_WH',
#     database='DSX_DB',
# )

# cur = con.cursor().execute(str(sqlCode))
# #Create and use pd dataframe
# df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
# df.to_csv(os.path.join(outputDir, jobName + '.csv'), index=False)