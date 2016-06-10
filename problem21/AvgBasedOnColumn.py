""" Finds average of each column 4(latitiude) based on column 2 (device model)
	EXAMPLE INPUT
	2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253
	2014-03-15:10:10:20,MeeToo,ef8c7564-0a1a-4650-a655-c8bbd5f8f943,37.4321088904,-121.485029632
"""
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

