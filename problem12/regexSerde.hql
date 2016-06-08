CREATE EXTERNAL TABLE  problem12 (
 ipp string,
 user_id string,
 dateall string,
 filename string,
 num1 string,
 num2 string,
 link string,
 libs string

  
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" =  "([\\d.]+) - (\\d+) \\[(.*?)\\] \\\"(.*?)\\\" (\\d+) (\\d+) \\\"(.*?)\\\"  \\\"(.*?)\\\"")
STORED AS TEXTFILE

LOCATION "/loudacre/weblogs" ;
