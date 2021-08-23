#importing module  
import sys
import pypyodbc as odbc
import pandas as pd

def get_connection(connstring):
    conn = odbc.connect(connstring)
    return conn
def get_tables(conn):
    cursor = conn.cursor()
    sql = "SELECT * FROM information_schema.TABLES order by table_schema,table_name"
    
    cursor.execute(sql)
    tables=cursor.fetchall()
    cursor.close()
    return tables
def get_tables(conn,table_schema):
    cursor = conn.cursor()  
    cursor.execute("SELECT TABLE_NAME FROM information_schema.TABLES where table_schema=? order by table_schema,table_name",[table_schema,])
    tables=cursor.fetchall()
    cursor.close()
    return tables
def print_tables(tables):
    for row in tables:
        print("{}.{}".format(row["table_schema"], row["table_name"])) 

def get_columns(conn, table_schema, table_name):
    """
    Creates and returns a list of dictionaries for the specified
    schema.table in the database connected to.
    """
    try:
        where_dict = {"table_schema": table_schema, "table_name": table_name}
        #where_dict = {"table_name": table_name}   
        cursor = conn.cursor()
        cursor.execute("""SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length
                          FROM information_schema.columns                     
                          WHERE table_schema = ?
                          AND table_name   = ?
                          ORDER BY ordinal_position""",[where_dict['table_schema'], where_dict['table_name'],])    
        columns = cursor.fetchall()
        cursor.close()  
        return columns
    except:
        e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )   
        return None

def print_columns(columns):

    """
    Prints the list created by get_columns.
    """
    for row in columns:
        print("Column Name:              {}".format(row["column_name"]))
        print("Ordinal Position:         {}".format(row["ordinal_position"]))
        print("Is Nullable:              {}".format(row["is_nullable"]))
        print("Data Type:                {}".format(row["data_type"]))
        print("Character Maximum Length: {}\n".format(row["character_maximum_length"])) 
def get_table(conn,table_schema, table_name):
    where_dict = {"table_schema": table_schema, "table_name": table_name}
    #where_dict = {"table_name": table_name}   
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM information_schema.TABLES                      
                        WHERE table_schema = ?
                          AND table_name   = ?
                          order by table_schema,table_name""",[where_dict['table_schema'], where_dict['table_name'],])    
    table= cursor.fetchall()
    cursor.close()      
    return table
def get_catalog_dataframe(catalog):
    conn={}
    try:
         connstr= "Driver={ODBC Driver 17 for SQL Server};Server=localhost\MSSQLSERVER01;" +"Database={};Trusted_Connection=yes;".format(catalog)
         conn = sdv.get_connection(connstr)
         sql="""SELECT                  
                                   TABLE_NAME ,
                                   COLUMN_NAME ,                                                     
                                   DATA_TYPE ,
                                   CHARACTER_MAXIMUM_LENGTH                            
                            FROM   INFORMATION_SCHEMA.COLUMNS
                            where TABLE_SCHEMA in ('dbo')
                            and table_name in (select name from sys.tables)
                            order by TABLE_SCHEMA , TABLE_NAME ,ORDINAL_POSITION"""
         df = pd.read_sql(sql, conn)
         return df
    except:
        e = sys.exc_info()[0]
        print( "<p>Error: %s</p>" % e )   
        return None
    finally:
        conn.close()


    
