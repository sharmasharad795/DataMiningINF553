import json
import re
import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext()

input_file=sys.argv[1]
output_file=sys.argv[2]
stopwords=sys.argv[3]
y=sys.argv[4]
m=sys.argv[5]
n=sys.argv[6]

res_t1={}

t1 = sc.textFile(input_file).map(lambda x: json.loads(x))
t1a = t1.map(lambda x:x["review_id"])
res_t1['A']=t1a.count()


def add(a,b):
    return a+b

t1b = t1.filter(lambda x:x["date"][0:4].find(y)!=-1)
res_t1['B']=t1b.count()


t1c = t1.map(lambda x:x["user_id"]).distinct()
res_t1['C']=t1c.count()

t1d = t1.map(lambda x:(x["user_id"],1)).reduceByKey(add).sortBy(lambda x:(-x[1],x[0])).map(lambda x:[x[0],x[1]])
res_t1['D']=t1d.take(int(m))


puncs=['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
replacer = re.compile('|'.join(map(re.escape, puncs)))
t1e = t1.map(lambda x:x["text"]).flatMap(lambda x: x.split(' ')).map(lambda x:x.lower()).map(lambda x: replacer.sub('',x))
stopWords = sc.textFile(stopwords)
d=stopWords.collect()
d.append('')
f=sc.parallelize(d)
res = t1e.subtract(f).map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x:(-x[1],x[0])).map(lambda x:(x[0]))
res_t1['E']=res.take(int(n))

with open(output_file,'w+') as f:
    json.dump(res_t1,f)



