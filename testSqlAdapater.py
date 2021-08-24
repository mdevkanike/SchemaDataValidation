import sqlAdpater as sqladpter
import pandas as pd
import pandasql
from pandasql import sqldf
import hashlib

def compare_schemas(source_catalog,source_schema,target_catalog,target_schema):    
     df1= pd.DataFrame(sqladpter.get_catalog_dataframe(source_catalog,source_schema))
     df2= pd.DataFrame(sqladpter.get_catalog_dataframe(target_catalog,target_schema))
     #df1.to_csv("C:\\Users\\mahad\\source\\repos\\SchemaDataValidation\\source_schema.csv")
     #df2.to_csv("C:\\Users\\mahad\\source\\repos\\SchemaDataValidation\\target_schema.csv")
     return df1.equals(df2)
#0
#sqladpter.get_catalog_to_csv('source_database','dbo')
#sqladpter.get_catalog_to_csv('target_database','dbo')

#1. Entity_schema validation
print("are source and target schemas are equal: {}".format(compare_schemas('source_database','dbo','target_database','dbo') ))

src_kpi_query="SELECT [GRADE] ,[LOSAL] ,[HISAL] FROM {}.{}".format('[dbo]',"[SALGRADE]")
#sqladpter.get_table_to_csv('source_database','dbo', 'SALGRADE',src_kpi_query)
#sqladpter.get_table_to_csv('target_database','dbo', 'SALGRADE',src_kpi_query)
#2 KPI_query
src_kpi_query="SELECT [GRADE] ,[LOSAL] ,[HISAL] FROM {}.{}".format('[dbo]',"[SALGRADE]")
source_df=pd.DataFrame(sqladpter.get_resultset_as_df('source_database','dbo',src_kpi_query) )

tgt_kpi_query="SELECT [GRADE] ,[LOSAL] ,[HISAL] FROM {}.{}".format('[dbo]',"[SALGRADE]")
target_df=pd.DataFrame(sqladpter.get_resultset_as_df('target_database','dbo',tgt_kpi_query))

print("are kpi queries of source and destination equal? = {}".format(source_df.equals(target_df)))

#3. DataCheck
pysqldf = lambda q: sqldf(q, globals())

# Source dataset
src_datacheck_query="SELECT  [EMPNO] ,[ENAME] ,[JOB] ,[MGR] ,[HIREDATE] ,[SAL] ,[COMM] ,[DEPTNO] FROM {}.{}".format('[dbo]',"[EMP]")
#sqladpter.get_table_to_csv('source_database','dbo', 'EMP',src_datacheck_query)
#sqladpter.get_table_to_csv('target_database','dbo', 'EMP',src_datacheck_query)
sdf=pd.DataFrame(sqladpter.get_resultset_as_df('source_database','dbo',src_datacheck_query) )
sdf['emp_details'] = sdf[sdf.columns[1:]].apply(lambda x: ','.join(x.dropna().astype(str)), axis=1)

sdf= pysqldf("SELECT empno,emp_details FROM sdf;")
new_sdf = sdf.copy()
key_combination = ['emp_details']
new_sdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_sdf[key_combination].values))
print(new_sdf.head())
#Target dataset
tgt_datacheck_query="SELECT [EMPNO] ,[ENAME] ,[JOB] ,[MGR] ,[HIREDATE] ,[SAL] ,[COMM] ,[DEPTNO] FROM {}.{}".format('[dbo]',"[EMP]")
tdf=pd.DataFrame(sqladpter.get_resultset_as_df('target_database','dbo',tgt_datacheck_query) )
tdf['emp_details'] = tdf[tdf.columns[1:]].apply(lambda x: ','.join(x.dropna().astype(str)),   axis=1)
tdf= pysqldf("SELECT empno,emp_details FROM tdf;")
new_tdf = tdf.copy()
key_combination = ['emp_details']
new_tdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_tdf[key_combination].values))
print(new_tdf.head())
#compare datasets 
print(pd.DataFrame(new_sdf).equals(pd.DataFrame(new_tdf)))
