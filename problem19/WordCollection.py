def listpartition(alist):
   
    final_iterator = []
    alist1 = list(alist)
    final_iterator = alist1[0:10]
    return iter(final_iterator)

htmlrdd = sc.textFile("/loudacre/kb*") \
	.mapPartitions(listpartition) \
	.map(lambda x:(x,1)) \
	.reduceByKey(lambda x,y: x + y)

htmlrdd.saveAsTextFile("/user/training/problem19/output")
