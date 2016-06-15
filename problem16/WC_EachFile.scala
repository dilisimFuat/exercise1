/*
This program read many files in a hdfs folder and prints number of each words of each file with their file names.
Example file: name /loudacre/kb/​KBDOC­00001.html 
Example output: KBDOC­00001.html|change|10 , this means KBDOC­00001.html file has 10 of 'change' words
*/

/**

To Run this file: 
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WC_EachFile{

	def main(args:Array[String]) = {

		if(args.length < 2 ){
      		System.exit(1)
		}
		val sc = new SparkContext()
		val totals = sc.wholeTextFiles(argv(0))
			.map(files => (files._1.split("/").last, files._2))
			.flatMapValues(content => content.split("\\s+"))
			.map(word => (word, 1)).reduceByKey(_+_)
			.map(line => line._1._1+"|"+line._1._2+"|"+line._2)

		totals.saveAsTextFile(argv(1))
		sc.stop()
    }
}
