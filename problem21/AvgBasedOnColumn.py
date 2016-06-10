devicestatus_etl = sc.textFile("/loudacre/devicestatus_etl") \
    .map(lambda line: line.split(',')) \
    .map(lambda line: (line[1], (float(line[3]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    
avg_of_each_device = devicestatus_etl \
    .map(lambda line: (line[0], line[1][0] / line[1][1]))    

print("model\tavg")
print("_____\t___")
for (model, avg) in avg_of_each_device.collect():
    print("%s\t%s"%(model,avg))

"""RESULTS
model	avg
_____	___
MeeToo	34.5593284325
Sorrento	34.567310076
Titanic	34.9045460681
Ronin	34.4867283166
iFruit	34.500989648
"""

