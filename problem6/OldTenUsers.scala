val accounts = sc.textFile("/user/training/problem5/output/accounts")

val account_dev = sc.textFile("/user/training/problem5/output/accountdevice")

val accounts1 = accounts.map(line => line.split('\t')).map(line =>( line(0),line))

val account_dev1 = account_dev.map(line => line.split('\t')).map(line =>( line(1),line))

val joinedAcc = accounts1.join(account_dev1)

val preSorted = joinedAcc.map(line =>(line._2._2(3), line._2._1(0)+"\t"+line._2._1(9)))

val sorted = preSorted.sortByKey()

//for( line <- sorted.take(10)) {
//	printf("%s %s\n",line._1,line._2)}

val sorted1 = sc.parallelize(sorted.takeOrdered(10))

val output = sorted1.map(line => line._2)

output.saveAsTextFile("/user/training/problem6/output")
