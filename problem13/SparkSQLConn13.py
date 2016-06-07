#python version of question 13
# coding: utf-8

# In[1]:

from pyspark.sql import SQLContext


# In[22]:
#imports functions like asc , desc etc
from pyspark.sql.functions import *


# In[2]:

sqlCtx = SQLContext(sc)


# In[3]:

sqlCtx


# In[7]:

df = sqlCtx.load(source='jdbc',url='jdbc:mysql://127.0.0.1:3306/information_schema?user=training&password=training', dbtable='COLUMNS')


# In[8]:

df.printSchema()


# In[24]:

#df.groupBy("COLUMN_NAME").count().orderBy(desc("count")).show()


# In[25]:

columnsrdd = sc.parallelize(df.groupBy("COLUMN_NAME").count().orderBy(desc("count")).collect())


# In[26]:

outputcolumnsrdd = columnsrdd.map(lambda line: line[0]+"\t"+`line[1]`)
outputcolumnsrdd.saveAsTextFile("/user/training/problem13/outputpy")


# In[ ]:



