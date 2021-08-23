/*https://dba.stackexchange.com/questions/95236/how-can-i-compare-the-schema-of-two-databases*/
SELECT TABLE_SCHEMA ,
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