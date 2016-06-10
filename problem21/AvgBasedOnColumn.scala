/*** Finds average of each column 4(latitiude) based on column 2 (device model)
	EXAMPLE INPUT
	2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253
	2014-03-15:10:10:20,MeeToo,ef8c7564-0a1a-4650-a655-c8bbd5f8f943,37.4321088904,-121.485029632
***/
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
