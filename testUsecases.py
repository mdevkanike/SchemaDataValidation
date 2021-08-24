import sqlAdpater as sqladpter
import pandas as pd
import pandasql
from pandasql import sqldf
import hashlib
import os
#1. Entity_schema validation
pysqldf = lambda q: sqldf(q, globals())
source_schema=pd.read_csv('source_database.csv');
target_schema=pd.read_csv('target_database.csv');
source_df= pysqldf("SELECT table_schema,table_name,column_name,ordinal_position,column_default,data_type,character_maximum_length,numeric_precision,numeric_precision_radix,numeric_scale,datetime_precision FROM source_schema;")
target_df= pysqldf("SELECT table_schema,table_name,column_name,ordinal_position,column_default,data_type,character_maximum_length,numeric_precision,numeric_precision_radix,numeric_scale,datetime_precision FROM target_schema;")
print("source_database.csv and target_database.csv schemas matched={}".format(source_df.equals(target_df)))
#2. KPI data check
source_kpi_df=pd.read_csv('source_database_dbo_SALGRADE.csv');
target_kpi_df=pd.read_csv('target_database_dbo_SALGRADE.csv');

print("are kpi queries of source and destination equal? = {}".format(source_kpi_df.equals(target_kpi_df)))

#3. DataCheck

# Source dataset

sdf=pd.read_csv('source_database_dbo_EMP.csv');
sdf['emp_details'] = sdf[sdf.columns[1:]].apply(lambda x: ','.join(x.dropna().astype(str)), axis=1)

sdf= pysqldf("SELECT empno,emp_details FROM sdf;")
new_sdf = sdf.copy()
key_combination = ['emp_details']
new_sdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_sdf[key_combination].values))
print(new_sdf.head())
#Target dataset
tdf=pd.read_csv('target_database_dbo_EMP.csv');
tdf['emp_details'] = tdf[tdf.columns[1:]].apply(lambda x: ','.join(x.dropna().astype(str)),   axis=1)
tdf= pysqldf("SELECT empno,emp_details FROM tdf;")
new_tdf = tdf.copy()
key_combination = ['emp_details']
new_tdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_tdf[key_combination].values))
print(new_tdf.head())
#compare datasets 
print(pd.DataFrame(new_sdf).equals(pd.DataFrame(new_tdf)))

