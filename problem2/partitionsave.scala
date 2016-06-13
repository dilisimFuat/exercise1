val rdd = sc.textFile("/user/training/problem1/output")
	val rddDrop = rdd.filter(line => line.contains("DROPPED")
	val rddFail = rdd.filter(line => line.contains("FAILED")
	val rddSucc = rdd.filter(line => line.contains("SUCCESS")

	rddDrop.saveAsTextFile("/user/training/problem2/output/status=DROPPED")
	rddFail.saveAsTextFile("/user/training/problem2/output/status=FAILED")
	rddSucc.saveAsTextFile("/user/training/problem2/output/status=SUCCESS")
