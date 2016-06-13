val htmlrdd = sc.textFile("/loudacre/kb/*")
	.mapPartitions(iterator => {val myList = iterator.toList; val firstTen = myList.take(10); firstTen.iterator})
	.map(x => (x,1)).reduceByKey(_+_)

htmlrdd.saveAsTextFile("/user/training/problem19/output")
