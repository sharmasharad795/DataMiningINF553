import json
import sys
import time
from itertools import combinations
import math 
from pyspark import SparkConf, SparkContext
import re
import csv
from collections import defaultdict
import itertools
from itertools import combinations
import pandas as pd
import numpy as np
import os

os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

sc=SparkContext()
sc.setLogLevel("ERROR")

input_file=sys.argv[1]
output_file=sys.argv[2]
biz_data_csv=sys.argv[3]
user_data_csv=sys.argv[4]
full_data=sys.argv[5]

start=time.time()


task3a=sc.textFile(input_file).map(lambda x: (json.loads(x)['user_id'],json.loads(x)['business_id'],json.loads(x)['stars'])).persist()

icf=task3a.map(lambda x:(x[0],x[1]))


business_user_set=icf.map(lambda x:(x[1],x[0])).groupByKey().map(lambda x:(x[0],list(set(x[1])))).collectAsMap()
business_pairs=list(set(icf.map(lambda x:x[1]).collect()))

def find_corated(k):
    biz1=set(business_user_set[k[0]])
    biz2=set(business_user_set[k[1]])
    co_rated=list(biz1 & biz2)
    no_co_rated=len(co_rated)
    return (co_rated,no_co_rated)
    
biz_pair=[]
biz_pair.append(business_pairs)
biz_sim=sc.parallelize(biz_pair).flatMap(lambda x : combinations(x, 2)).map(lambda x:((x[0],x[1]),find_corated(x))).filter(lambda x:x[1][1]>=3).map(lambda x:(x[0],x[1][0])).persist()



def aggregator(bizlist):
    bizdic={}
    for x in bizlist:
        if x[0] not in bizdic:
            bizdic[x[0]]=[x[1]]
        else:
            bizdic[x[0]].append(x[1])
    for key in bizdic:
        bizdic[key] = sum(bizdic[key])/len(bizdic[key])
    
    return bizdic


user_business_map=task3a.map(lambda x: (x[0],(x[1],x[2]))).groupByKey().map(lambda x:(x[0],list(x[1]))).map(lambda x:(x[0],aggregator(x[1]))).collectAsMap()  

def pearson_sim(idk):
    biz1=idk[0][0]
    biz2=idk[0][1]
    sum_biz1=0
    sum_biz2=0
    
    for i in idk[1]:
        sum_biz1=sum_biz1+user_business_map[i][biz1]
        sum_biz2=sum_biz2+user_business_map[i][biz2]
        
    avg_biz1=sum_biz1/len(idk[1])
    avg_biz2=sum_biz2/len(idk[1])
    
    numerator_sum=0    
    for i in idk[1]:
        numerator_mul=1
        numerator_mul=numerator_mul*(user_business_map[i][biz1]-avg_biz1)
        numerator_mul=numerator_mul*(user_business_map[i][biz2]-avg_biz2)
        numerator_sum=numerator_sum+numerator_mul

    deno_mul=1
    counter=0
    for a in idk[0]:
        biz1_root=0
        if counter==0:
            for b in idk[1]:
                biz1_root=biz1_root+((user_business_map[b][a]-avg_biz1)**2)
            counter+=1
        else:
            for b in idk[1]:
                biz1_root=biz1_root+((user_business_map[b][a]-avg_biz2)**2)
                
            
        
        deno_mul=deno_mul*math.sqrt(biz1_root)
    
    if deno_mul==0:
        return 0
    else:
        res=numerator_sum/deno_mul
        return res
        
final_ouput=biz_sim.map(lambda x:((x[0][0],x[0][1]),pearson_sim(x))).filter(lambda x:(x[1]>0)).collect()

with open(output_file, "w+") as f:
   
    for x in final_ouput:
        res_dic={"b1":x[0][0],"b2":x[0][1],"sim":x[1]}
        json.dump(res_dic, f)
        f.write('\n')
        
biz2=pd.read_json(input_file, lines=True)
biz2=biz2[['user_id', 'business_id', 'stars']]

user_grouped=biz2.groupby('user_id')
user_count=user_grouped.agg('count')
del user_count['business_id']
user_count=user_count.reset_index()
user_count.columns=['user_id','user_review_count']
avg_stars_user=user_grouped['stars'].aggregate(np.mean)
avg_stars_user=avg_stars_user.to_frame()
avg_stars_user=avg_stars_user.reset_index()
avg_stars_user.columns=['user_id','user_avg_stars']
user_data=user_count.merge(avg_stars_user,on='user_id',how='inner')

biz_grouped=biz2.groupby('business_id')
biz_count=biz_grouped.agg('count')
del biz_count['user_id']
biz_count=biz_count.reset_index()
biz_count.columns=['business_id','biz_review_count']
avg_stars_biz=biz_grouped['stars'].aggregate(np.mean)
avg_stars_biz=avg_stars_biz.to_frame()
avg_stars_biz=avg_stars_biz.reset_index()
avg_stars_biz.columns=['business_id','biz_avg_stars']
biz_data=biz_count.merge(avg_stars_biz,on='business_id',how='inner')

biz_data.to_csv(biz_data_csv)
user_data.to_csv(user_data_csv)

biz2=biz2.merge(user_data,how='inner',on='user_id')
biz2=biz2.merge(biz_data,how='inner',on='business_id')

biz2.to_csv(full_data)

end=time.time()
print('Duration:',end-start)
    