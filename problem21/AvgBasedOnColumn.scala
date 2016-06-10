val devicestatus_etl = sc.textFile("/loudacre/devicestatus_etl")
	.map(line => line.split(","))
	.map(line => (line(1),( line(3).toDouble, 1)))
	.reduceByKey((x, y) => (x._1+y._1, x._2 + y._2))

val avg_of_each_device = devicestatus_etl
	.map(line => (line._1, line._2._1 / line._2._2))

devicestatus_etl.collect().foreach(println)
/*RESULTS
(Titanic,(3323610.8766045626,95220))
(MeeToo,(2164796.333009458,62640))
(Ronin,(1427750.5523086071,41400))
(iFruit,(1651907.3843613525,47880))
(Sorrento,(7342096.660143794,212400))
*/
