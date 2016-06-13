rdd = sc.textFile("/user/training/problem1/output")
rddDropped = rdd.filter(lambda line: line.split("\t")[3] == "DROPPED")
rddSuccess = rdd.filter(lambda line: line.split("\t")[3] == "SUCCESS")
rddFailed = rdd.filter(lambda line: line.split("\t")[3] == "FAILED")

rddDropped.saveAsTextFile("/user/training/problem2/output/status=DROPPED")
rddFailed.saveAsTextFile("/user/training/problem2/output/status=FAILED")
rddSuccess.saveAsTextFile("/user/training/problem2/output/status=SUCCESS")
