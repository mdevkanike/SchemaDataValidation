SELECT Distinct TABLE_NAME FROM information_schema.TABLES
SELECT *
                      FROM information_schema.tables
                      WHERE table_schema != 'pg_catalog'
                      AND table_schema != 'information_schema'
                      AND table_type='BASE TABLE'

SELECT 
TABLE_CATALOG,
TABLE_SCHEMA,
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
where TABLE_SCHEMA in ('dbo','meta')
and table_name in (select name from sys.tables)
order by TABLE_SCHEMA ,       TABLE_NAME ,ORDINAL_POSITION


SELECT * FROM information_schema.TABLES where table_name ='DEPT' order by table_schema,table_name
SELECT TABLE_NAME FROM information_schema.TABLES where table_schema='dbo' order by table_schema,table_name