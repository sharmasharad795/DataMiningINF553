import json
import re
import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext()

review_file=sys.argv[1]
output_file=sys.argv[2]
partition_type=sys.argv[3]
n_partitions=int(sys.argv[4])
n=int(sys.argv[5])

res_t3 = {}
def add(a,b):
    return a+b
if partition_type == "default":
   
    t1 = sc.textFile(review_file).map(lambda x: json.loads(x))
    t1 = t1.map(lambda x:(x['business_id'],1)).reduceByKey(add).filter(lambda x:x[1]>n).map(lambda x:[x[0],x[1]])
    items_part=t1.glom().map(len).collect()
    no_of_parts=t1.getNumPartitions()
    res_t3['n_partitions']=no_of_parts
    res_t3['n_items']=items_part
    res_t3['result']=t1.collect()
 
else:
    def helper(business_id):
        return hash(business_id)
    
    t2 = sc.textFile(review_file).map(lambda x: json.loads(x)).persist()
    t2 = t2.map(lambda x: (x['business_id'], 1)).partitionBy(n_partitions,helper).reduceByKey(add).filter(lambda x:x[1]>n).map(lambda x:[x[0],x[1]])
    items_part=t2.glom().map(len).collect()
    no_of_parts=t2.getNumPartitions()
    res_t3['n_partitions']=no_of_parts
    res_t3['n_items']=items_part
    res_t3['result']=t2.collect()
    
with open(output_file,'w+') as f:
    json.dump(res_t3,f)
    

