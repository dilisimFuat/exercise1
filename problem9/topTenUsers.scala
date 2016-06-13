val chatlogDF = sqlContext.load("/user/training/problem8/output/chatlogs/*", "json") 

val chatlogRDD = chatlogDF.rdd

val filtered2 = chatlogRDD.filter(row => row(2).equals("Device")).map(line=> line(1))

val groupbycountRDD = filtered2.map(line => (line, 1)).reduceByKey(_+_)

val reverse = groupbycountRDD.map(line => (line._2, line._1)).sortByKey(ascending=false)
val rake10 = sc.parallelize(reverse.take(10)).persist()

val jsonCreator = rake10.map(line => "{\"agentName\":\""+line._2+"\", \"ctgNum\":\""+line._1+"\" }")

jsonCreator.saveAsTextFile("/user/training/problem9/output")

/*RESULTS
{"agentName":"Rachel Riley", "ctgNum":"987" }
{"agentName":"Gregory Barber", "ctgNum":"956" }
{"agentName":"Renee Sanders", "ctgNum":"947" }
{"agentName":"Carolyn Jones", "ctgNum":"939" }
{"agentName":"Zackary Ostler", "ctgNum":"937" }
{"agentName":"Beverly Battaglia", "ctgNum":"933" }
{"agentName":"Donald Christopher", "ctgNum":"933" }
{"agentName":"Aaron Henderson", "ctgNum":"931" }
{"agentName":"Andrew Murphy", "ctgNum":"928" }
{"agentName":"Edward Floyd", "ctgNum":"927" }

*/
