#Finds average call time based on status.
MSCK REPAIR TABLE calllog;

#OR WE CAN UPLOAD DATA IN THE PATH AS BELOW

LOAD DATA INPATH '/user/training/problem2/output/status=DROPPED'
INTO TABLE calllog PARTITION (status='DROPPED') 

LOAD DATA INPATH '/user/training/problem2/output/status=FAILED'
INTO TABLE calllog PARTITION (status='FAILED') 

LOAD DATA INPATH '/user/training/problem2/output/status=SUCCESS'
INTO TABLE calllog PARTITION (status='SUCCESS') 

SELECT status, substr(avg(unix_timestamp(end_time)-unix_time_stamp(start_time)), 1, 5) as timediff
FROM calllog
GROUP BY status
