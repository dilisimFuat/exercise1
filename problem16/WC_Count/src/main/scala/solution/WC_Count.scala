package solution16

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WC_Count {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: WC_EachFile <in file> <out file>")
       System.exit(1)
     }

     val sc = new SparkContext()
     
     

     val totals = sc.wholeTextFiles(args(0))
			.map(files => (files._1.split("/").last, files._2))
			.flatMapValues(content => content.split("\\s+"))
			.map(word => (word, 1)).reduceByKey(_+_)
			.map(line => line._1._1+"|"+line._1._2+"|"+line._2)

	 totals.saveAsTextFile(args(1))

     sc.stop
   }
 }

