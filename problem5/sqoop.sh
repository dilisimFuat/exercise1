 sqoop import
 --connect jdbc:mysql://localhost:3306/loudacre
 --username training --password training 
 --table accountdevice
 --target-dir /user/training/problem5/output/accountdevice
 --fields-terminated-by '\t' 


 sqoop import 
 --connect jdbc:mysql://localhost:3306/loudacre 
 --username training --password training 
 --table accounts
 --target-dir /user/training/problem5/output/accounts 
 --fields-terminated-by '\t
