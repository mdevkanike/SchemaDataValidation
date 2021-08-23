import SchemaDataValidation as sdv
import pandas as pd

def get_dataframes(database,table_schema):
    connstr= "Driver={ODBC Driver 17 for SQL Server};"+"Server=localhost\MSSQLSERVER01;Database={};Trusted_Connection=yes;".format(database)
    conn = odbc.connect(connstr)
    tables=sdv.get_tables(conn,table_schema)
     #sdv.print_tables(tables)
    dataframes={}
    #read_columns
    for table in tables:
            dataframes[table["table_name"]] = pd.DataFrame(data=sdv.get_table(conn,table_schema,table["table_name"]))
    conn.close()
    return dataframes
def compare_schemas(schema1,schema2):
    retVal = False
    connstr= "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;Database=source_database;Trusted_Connection=yes;"
    conn = sdv.get_connection(connstr)
    tables1=sdv.get_tables(conn,schema1)
    tables2=sdv.get_tables(conn,schema2)
    if tables1.count != tables2.count:
        return retVal
    for table in tables1:
       df1 = pd.DataFrame(sdv.get_table(conn,schema1,table["table_name"]))
       df2 = pd.DataFrame(sdv.get_table(conn,schema2,table["table_name"]))
       if df1.equals(df2):
           return retVal
 
    return True
def compare_schemas2(schema1,schema2):
     dataframes1=get_dataframes('source_database','dbo')
     dataframes2=get_dataframes('target_database','dbo')
     for key in dataframes1:
         print(key,'->',dataframes1[key])
     retVal= True
     keys = dataframes1.keys
     for key in keys:
         df1 = pd.DataFrame(dataframes1[key])
         df2 = pd.DataFrame(dataframes2[key])
         if df1.equals(df2):
             retVal= False
             break
     print(retVal)
def compare_schemas3(source,target):    
     df1= pd.DataFrame(get_catalog_dataframe(source))
     df2= pd.DataFrame(get_catalog_dataframe(target))
     return df1.equals(df2)
 


# 1. EntityCheck
compare_schemas3('source_database','target_database')
#display_schema('source_database')

