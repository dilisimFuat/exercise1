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
from bs4 import BeautifulSoup
totals = sc.wholeTextFiles("/loudacre/kb*") \
    	.map(lambda (fname, content): (str(fname.split("/")[-1]), content)) \
    	.map(lambda (fname,line):  (fname,BeautifulSoup(line, 'html.parser').get_text().strip())) \
    	.flatMapValues(lambda x: x.split()) \
    	.map(lambda x: (x, 1)) \
    	.reduceByKey(lambda x, y: x + y) \
     	.map(lambda ((fname,word), count): fname+"|"+word+"|"+`count`).take(10)

totals.saveAsTextFile("/user/training/problem16/")

