/**
val htmlrdd = sc.textFile("/loudacre/kb/*")
val rdd = htmlrdd.mapPartitionsWithIndex{
      (files, iterator) => {
      //println("Partition -> "+files)
      val myList = iterator
.toList
	  val firstrow = myList(0)
      //myList.map(x => x + " -> "+files).iterator
	   myList.map{x => 
		{
			if(x.equals(firstrow))
				//(firstrow, 1)
				1
			else
				//(firstrow, 0)
				0
		}
	   }.iterator

      }
      }
#rdd.sum()
*/

rdd.saveAsTextFile("/loudacre/mapPartittion2")
