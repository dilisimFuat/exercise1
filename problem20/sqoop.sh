#Creates the problem20 with one command.
sqoop import \
--connect jdbc:mysql://localhost/information_schema \
--username training -P \
--table columns \
--null-non-string '\\N' \
--columns "COLUMN_NAME,DATA_TYPE,IS_NULLABLE" \
--where "table_schema='hue'" \
--hive-table problem20 \
--hive-import \
-m 1
