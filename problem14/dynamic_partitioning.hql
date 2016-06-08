CREATE TABLE  problem14 (
 ipp string,
 user_id string,
 filename string,
 num1 string,
 num2 string,
 link string,
 libs string
)

PARTITIONED BY(byDay string)
STORED AS PARQUET
LOCATION "/user/training/problem14/output";

#set this to nonstrict in order to allow dynamic partitioning
set hive.exec.dynamic.partition.mode=nonstrict

INSERT INTO problem14 
PARTITION(byDay)
SELECT ipp, user_id, filename, num1, num2, link, libs, split(dateall, '/')[0] as byDay
FROM problem12;

#SELECT ipp, user_id, filename, num1, num2, link, libs, substr(dateall, 1, 2)
#FROM problem12 LIMIT 1;
