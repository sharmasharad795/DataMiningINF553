import json
import binascii
import math
import random
import time
import datetime
import sys
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext


sc=SparkContext()
sc.setLogLevel("ERROR")

port=int(sys.argv[1])
op_file=sys.argv[2]

list1=[173, 93, 77, 49, 142, 62, 71, 155, 88, 66, 83, 26, 132, 98, 28, 110, 183, 185, 2, 164, 40, 78, 64, 180, 141, 117, 135, 69, 90, 38, 67, 17, 157, 166, 76, 47, 15, 104, 27, 65, 186, 95, 151, 138, 24, 14, 58, 102, 74, 195] 
list2=[309, 389, 298, 269, 266, 344, 284, 237, 330, 240, 236, 322, 367, 349, 300, 314, 319, 388, 315, 268, 375, 257, 387, 313, 378, 336, 259, 360, 260, 270, 362, 352, 293, 282, 267, 253, 301, 262, 244, 289, 331, 248, 356, 343, 334, 397, 206, 335, 364, 366]


hash_variables=[]

for i,j in list(zip(list1,list2)):
    hash_variables.append((i,j))


def mapper1(chunk):
    f = open(op_file, "a+")
    chunker=chunk.collect()
    hash_max=[]
    city_set=set()
    for x in range(0,50):
        hash_max.append(-1)
    for data in chunker:
        city=json.loads(data)['city']
        city_set.add(city)
        city_to_int=int(binascii.hexlify(city.encode('utf8')),16)
        for i in range(0,len(hash_variables)):
            a=hash_variables[i][0]
            b=hash_variables[i][1]
            city_hash=((a*city_to_int)+b)%12289
            
            city_to_binary='{0:09b}'.format(city_hash)
            city_trailingzero=len(city_to_binary) - len(city_to_binary.rstrip('0'))
            if city_trailingzero>hash_max[i]:
                hash_max[i]=city_trailingzero
                
    hash_max_count=[2**x for x in hash_max ]
    l=list(zip(hash_max_count[::2],hash_max_count[1::2]))
   
    
    avg=[]
    for x in l:
        avg.append(sum(x)/len(x))
    
    avg.sort()
    median1 = avg[len(avg)//2] 
    median2 = avg[len(avg)//2 - 1] 
    median = (median1 + median2)/2

    gt=len(city_set)
    f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "," + str(gt) + "," + str(median)+ "\n")
    f.close()
      
    
      
f = open(op_file, "w+")
f.write("Time,Gound Truth,Estimation\n")
f.close() 

    
ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", port)
stream1=lines.window(30,10)
stream1.foreachRDD(mapper1)
ssc.start()
ssc.awaitTermination()