import sqlAdpater as sqladpter
import pandas as pd
import pandasql
from pandasql import sqldf
import hashlib

def compare_schemas(source_catalog,source_schema,target_catalog,target_schema):    
     df1= pd.DataFrame(sqladpter.get_catalog_dataframe(source_catalog,source_schema))
     df2= pd.DataFrame(sqladpter.get_catalog_dataframe(target_catalog,target_schema))
     return df1.equals(df2)
pysqldf = lambda q: sqldf(q, globals())
#1. Entity_schema validation
print("are source and target schemas are equal: {}".format(compare_schemas('source_database','dbo','target_database','dbo') ))

#2 KPI_query
src_kpi_query="SELECT [GRADE] ,[LOSAL] ,[HISAL] FROM {}.{}".format('[dbo]',"[SALGRADE]")
source_df=pd.DataFrame(sqladpter.get_resultset_as_df('source_database','dbo',src_kpi_query) )

tgt_kpi_query="SELECT [GRADE] ,[LOSAL] ,[HISAL] FROM {}.{}".format('[dbo]',"[SALGRADE]")
target_df=pd.DataFrame(sqladpter.get_resultset_as_df('target_database','dbo',tgt_kpi_query))

print("are kpi queries of source and destination equal? = {}".format(source_df.equals(target_df)))

#3. DataCheck

src_datacheck_query="SELECT TOP 1000 [EMPNO] ,[ENAME] ,[JOB] ,[MGR] ,[HIREDATE] ,[SAL] ,[COMM] ,[DEPTNO] FROM {}.{}".format('[dbo]',"[EMP]")
sdf=pd.DataFrame(sqladpter.get_resultset_as_df('source_database','dbo',src_datacheck_query) )
sdf['emp_details'] = sdf[sdf.columns[1:]].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1
)
#sdf.drop(['ENAME','JOB','MGR','HIREDATE','SAL','COMM','DEPTNO' ],axis=1)
#print("Source database modified dataframe: ")
sdf= pysqldf("SELECT empno,emp_details FROM sdf;")
#print(sdf.head())
new_sdf = sdf.copy()
key_combination = ['emp_details']
new_sdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_sdf[key_combination].values))
print(new_sdf.head())
tgt_datacheck_query="SELECT TOP 1000 [EMPNO] ,[ENAME] ,[JOB] ,[MGR] ,[HIREDATE] ,[SAL] ,[COMM] ,[DEPTNO] FROM {}.{}".format('[dbo]',"[EMP]")
tdf=pd.DataFrame(sqladpter.get_resultset_as_df('source_database','dbo',tgt_datacheck_query) )
tdf['emp_details'] = tdf[tdf.columns[1:]].apply(
    lambda x: ','.join(x.dropna().astype(str)),
    axis=1)
new_tdf = tdf.copy()
key_combination = ['emp_details']
new_tdf.index = list(map(lambda x: hashlib.sha1('-'.join([col_value for col_value in x]).encode('utf-8')).hexdigest(), new_tdf[key_combination].values))
print(new_tdf.head())

#print("Target database modified dataframe: ")
#print(pysqldf("SELECT empno,emp_details FROM tdf;").head())