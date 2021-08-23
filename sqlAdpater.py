import sys
import pypyodbc as odbc
import pandas as pd

def get_catalog_dataframe(catalog, schema):    
    try:
         connstr= "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;"+"Database={};Trusted_Connection=yes;".format(catalog)
         conn = odbc.connect(connstr)
         sql="""SELECT                  
                            TABLE_NAME ,
                            COLUMN_NAME ,
                            ORDINAL_POSITION ,
                            COLUMN_DEFAULT ,
                            DATA_TYPE ,
                            CHARACTER_MAXIMUM_LENGTH ,
                            NUMERIC_PRECISION ,
                            NUMERIC_PRECISION_RADIX ,
                            NUMERIC_SCALE ,
                            DATETIME_PRECISION                            
                            FROM   INFORMATION_SCHEMA.COLUMNS
                            where TABLE_SCHEMA in ('{}')
                            and table_name in (select name from sys.tables)
                            order by TABLE_SCHEMA , TABLE_NAME ,ORDINAL_POSITION""".format(schema)
         df = pd.read_sql(sql, conn)
         conn.close()
         return df
    except:
        e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )   
        return None
        conn.close()

def get_resultset_as_df(catalog,schema,kpi_query):
    try:
        connstr= "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;"+"Database={};Trusted_Connection=yes;".format(catalog)
        conn = odbc.connect(connstr)        
        df = pd.read_sql(kpi_query, conn)
        conn.close()
        return df
    except:
        e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )   
        return None
        conn.close()

