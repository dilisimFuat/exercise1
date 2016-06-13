#create a output and transfer chatlog files exactly under output.
hdfs dfs -mkdir -p /user/training/problem8/output

hdfs dfs -mkdir /user/training/problem8/output
hdfs dfs -put chatlogs/* /user/training/problem8/output
