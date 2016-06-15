'''
To run this, html parser, beautiful-soup need to be installed, downloading instructions can be found link below.
https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-beautiful-soup
This program read many files in a hdfs folder and prints number of each words of each file with their file names.
Example file: name /loudacre/kb/​KBDOC­00001.html 
Example output: KBDOC­00001.html|change|10 , this means KBDOC­00001.html file has 10 of 'change' words

'''
'''from bs4 import BeautifulSoup
from operator import add
def wcount(fname):    
    lines = sc.textFile("/loudacre/kb/"+fname)
    counts = lines.map(lambda line:  BeautifulSoup(line, 'html.parser').get_text().strip()).flatMap(lambda x: x.split()) \
				  .map(lambda x: (x, 1))   \                
				  .reduceByKey(add).map(lambda (word,count): fname+"|"+word+"|"+`count`)
    return sc.parallelize(counts.collect()) 
htmlrdd = sc.wholeTextFiles("/loudacre/kb/*").map(lambda (fname, content): str(fname.split("/")[-1]))
totals = sc.parallelize([])
for i in htmlrdd.collect():
    totals = totals.union(wcount(i))
'''

#To Run spark-submit WC_EachFile.py <in-file> <out-file>
from bs4 import BeautifulSoup
from pyspark import SparkContext
import sys

if __name__ == "__main__":
	if(len(sys.argv) < 3):
		print("Usage: This file <input_file> <output_file>", file=sys.stderr)
        exit(-1)

	sc = SparkContext()
	totals = sc.wholeTextFiles(sys.argv[1]) \
			.map(lambda (fname, content): (str(fname.split("/")[-1]), content)) \
			.map(lambda (fname,line):  (fname,BeautifulSoup(line, 'html.parser').get_text().strip())) \
			.flatMapValues(lambda x: x.split()) \
			.map(lambda x: (x, 1)) \
			.reduceByKey(lambda x, y: x + y) \
		 	.map(lambda ((fname,word), count): fname+"|"+word+"|"+`count`).take(10)

	totals.saveAsTextFile(sys.argv[2])
	sc.stop()

