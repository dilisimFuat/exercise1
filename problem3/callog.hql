 CREATE EXTERNAL TABLE calllog (
  
  call_num STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  from_phone_number STRING,
  to_phone_number STRING
  
  ) 
  PARTITIONED BY (status STRING)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LOCATION '/user/training/problem2/calllog'
