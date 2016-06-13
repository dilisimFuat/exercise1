sqoop eval \
--connect jdbc:mysql://localhost:3306/loudacre 
--username training --password training 
--query "CREATE TABLE loudacre.oldestactivations(acct_num int(11), phone_number varchar(255))"



sqoop export 
--connect jdbc:mysql://localhost:3306/loudacre 
--username training --password training 
--table oldestactivations 
--export-dir /user/training/problem6/output 
--fields-terminated-by '\t'
