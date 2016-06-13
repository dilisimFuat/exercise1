def limit(rdd:RDD, n:Int):RDD[String] = {

	return sc.parallelize(rdd.take(10))

}
