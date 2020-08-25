import json
import re
import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext()

review_file=sys.argv[1]
business_file=sys.argv[2]
output_file=sys.argv[3]
if_spark=sys.argv[4]
n=int(sys.argv[5])
Highest_star_Categories={}

if if_spark=='no_spark':
    
    from collections import defaultdict

    dic1=defaultdict(list)
    dic2=defaultdict(list)

    with open(business_file,'r') as f:
        bizfile=f.readlines()
    
    bfile=[]
    for bizvalues in bizfile:
        bizvalues=json.loads(bizvalues)
        if bizvalues['categories'] is not None:
            cat=bizvalues['categories'].strip()
            cat=bizvalues['categories'].split(',')
            for x in cat:
                x=x.strip()
                bfile.append((bizvalues['business_id'],x))
            
    with open(review_file,'r') as f:
        revfile=f.readlines()
    
    rfile=[]

    for rvals in revfile:
        rvals=json.loads(rvals)
        rfile.append((rvals['business_id'],rvals['stars']))
    
    
    for x,y in bfile:
   
        dic1[x].append(y)
     
    for x in rfile:
        if x[0] in dic1.keys():
            for y in dic1[x[0]]:
                dic2[y].append(x[1])
        

    res=[]



    for x,v in dic2.items():
        res.append([x,sum(v)/len(v)])
    
    res.sort(key=lambda x:x[0])
    res.sort(key=lambda x:x[1],reverse=True)
    
    ans=res[:n]
    Highest_star_Categories['result']=ans


 
else:
    
    
    t1 = sc.textFile(review_file).map(lambda x: json.loads(x))
    t2 = sc.textFile(business_file).map(lambda x: json.loads(x))
    ta = t2.map(lambda x:(x['business_id'],x['categories'])).filter(lambda s:s[1] is not None).flatMapValues(lambda x:x.split(',')).mapValues(lambda x:x.strip())
    tb = t1.map(lambda x:(x["business_id"],x["stars"]))
    tc = ta.join(tb).map(lambda x:(x[1][0],x[1][1])).aggregateByKey((0,0),lambda a,b:(a[0]+b,a[1]+1),lambda a,b:(a[0]+b[0],a[1]+b[1])).map(lambda x:(x[0],(float(x[1][0])/x[1][1]))).sortBy(lambda x:(-x[1],x[0])).map(lambda x:[x[0],x[1]])
   
    Highest_star_Categories['result']=tc.take(int(n))
    
with open(output_file,'w+') as f:
    json.dump(Highest_star_Categories,f)