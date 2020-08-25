import os
import pyspark
from graphframes import GraphFrame
from pyspark.sql import SQLContext
from pyspark import SparkContext,SparkConf
import sys
from itertools import combinations
import time
from pyspark.sql import SparkSession

start = time.time()
Conf = SparkConf().setAppName('assgn4').setMaster('local[3]').set('spark.jars.packages','graphframes:graphframes:0.6.0-spark2.3-s_2.11')


sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]



df1=sc.textFile(input_file).persist()
header = df1.first()
df1=df1.filter(lambda x: x != header).map(lambda x: (str(x.split(',')[0]), str(x.split(',')[1]))).persist()
user_biz_map=df1.groupByKey().mapValues(list).collectAsMap()

users=user_biz_map.keys()
pair_nodes=list(combinations(users,2))


ans=[]
for i in pair_nodes:
    biz1=user_biz_map[i[0]]
    biz2=user_biz_map[i[1]]
    inter = len(set(biz1)&set(biz2))
    if inter>=threshold:
        ans.append(((i[0],i[1]),inter))
        ans.append(((i[1],i[0]),inter))
    
candidate_nodes=sc.parallelize(ans).map(lambda x:(x[0][0],x[0][1])).persist()


edges = sqlContext.createDataFrame(candidate_nodes).toDF("src", "dst")


vertex=candidate_nodes.map(lambda x:x[0]).distinct().map(lambda x:[x])
vertices = sqlContext.createDataFrame(vertex).toDF("id")
g = GraphFrame(vertices, edges)

result = g.labelPropagation(maxIter=5)

result_=result.rdd.map(lambda x:(x['label'],x['id'])).groupByKey().mapValues(list).map(lambda x:(sorted(x[1]),len(x[1]))).sortBy(lambda x:(x[1],x[0][0])).map(lambda x:x[0]).collect()

with open(output_file,'w') as f:
    for x in result_:
        f.write(str(x)[1:-1])
        f.write("\n")
    

end=time.time()
print("Duration:",end-start)