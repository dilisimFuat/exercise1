/*
This program read many files in a hdfs folder and prints number of each words of each file with their file names.
Example file: name /loudacre/kb/​KBDOC­00001.html 
Example output: KBDOC­00001.html|change|10 , this means KBDOC­00001.html file has 10 of 'change' words
*/

val totals = sc.wholeTextFiles("/loudacre/kb/*")
	.map(files => (files._1.split("/").last, files._2))
	.flatMapValues(content => content.split("\\s+"))
	.map(word => (word, 1)).reduceByKey(_+_)
	.map(line => line._1._1+"|"+line._1._2+"|"+line._2)

totals.saveAsTextFile("/user/training/problem16/")