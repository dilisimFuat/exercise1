/**
This program connect Spark to mysql database on local machine and processing the given table.

In order to connect any db, the spark-shell should be run with the target database connector, for example spark with mysql can be integrated as below.

The connector/driver must be obtained before running the command below.
**MySQL JDBC driver (download available https://dev.mysql.com/downloads/connector/j/)**
SPARK_CLASSPATH=mysql-connector-java-5.1.39-bin.jar spark-shell
*/


import org.apache.spark.sql.SQLContext
//used for importing dataframe methods like sort() method.
import org.apache.spark.sql.DataFrame

var sqlCtx = new SQLContext(sc)
//Check if mysql driver successfully works 
//Class.forName("com.mysql.jdbc.Driver").newInstance
val df = sqlCtx.load("jdbc", Map("url" -> "jdbc:mysql://127.0.0.1:3306/information_schema?user=training&password=training", "dbtable" -> "COLUMNS"))
//df.printSchema()
//1. way of solving the problem.
//df.groupBy("COLUMN_NAME").count().sort($"count".desc).show()

val columnsrdd = sc.parallelize(df.groupBy("COLUMN_NAME").count().orderBy($"count".desc).collect())
//remove square brackets from the columnsrdd
val outputcolumnsrdd = columnsrdd.map(line => line(0)+"\t"+line(1))
outputcolumnsrdd.saveAsTextFile("/user/training/problem13/output")

