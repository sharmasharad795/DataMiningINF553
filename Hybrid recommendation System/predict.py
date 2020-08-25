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
import numpy
import os
import xgboost as xgb

os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

sc=SparkContext()
sc.setLogLevel("ERROR")


test_file_path=sys.argv[1]
output_file=sys.argv[2]
model_file='model_file.json'
train_file='../resource/asnlib/publicdata/train_review.json'
biz_data=pd.read_csv('biz_data.csv')
user_data=pd.read_csv('user_data.csv')
biz2=pd.read_csv('full_data.csv')


start=time.time()

user_avg=user_data['user_avg_stars'].mean()
business_avg=biz_data['biz_avg_stars'].mean()

task3_model_mapping=sc.textFile(model_file).map(lambda x: ((json.loads(x)['b1'],json.loads(x)['b2']),json.loads(x)['sim'])).collectAsMap()
test_file=sc.textFile(test_file_path).map(lambda x: (json.loads(x)['user_id'],json.loads(x)['business_id'])).persist()


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

train_file=sc.textFile(train_file).map(lambda x: (json.loads(x)['user_id'],(json.loads(x)['business_id'],json.loads(x)['stars']))).groupByKey().mapValues(list).map(lambda x:(x[0],aggregator(x[1]))).persist()              

test_train_join = test_file.join(train_file)

#####################deals with empty users##############
get_users=test_train_join.map(lambda x:x[0]).distinct()
test_users=test_file.map(lambda x:x[0]).distinct()
user_left_out=test_users.subtract(get_users)

user_left_out_biz_avg=biz_data[['business_id','biz_avg_stars']]
user_left_out_biz_avg=user_left_out_biz_avg.set_index('business_id').T.to_dict('dict')

def get_missing_user(biz,dicti):
    if biz in dicti:
        return dicti[biz]['biz_avg_stars']
    
user_get_avg=user_left_out.map(lambda x:(x,1)).join(test_file).map(lambda x:(x[0],x[1][1])).map(lambda x:(x,get_missing_user(x[1],user_left_out_biz_avg)))

temp=user_get_avg.map(lambda x:x[0][0])
user_get_avg=user_get_avg.map(lambda x:(x[0][0],x[0][1],x[1])).collect()
anybody_left=user_left_out.subtract(temp).map(lambda x:(x,1)).join(test_file).map(lambda x:((x[0],x[1][1]),business_avg)).map(lambda x:(x[0][0],x[0][1],x[1])).collect()


biz_left_out_user_avg=user_data[['user_id','user_avg_stars']]
biz_left_out_user_avg=biz_left_out_user_avg.set_index('user_id').T.to_dict('dict')

####################

def mapper(x,user_dic):

    userid,bizid,sim_business = x[0],x[1][0],x[1][1]
    similarity = []
    for j in sim_business:
        if(bizid,j) in task3_model_mapping: 
            similarity.append((task3_model_mapping[(bizid,j)],sim_business[j]))
        if (j,bizid) in task3_model_mapping:
            similarity.append((task3_model_mapping[(j,bizid)],sim_business[j]))
    if len(similarity)==0:
        if userid in user_dic:
            return user_dic[userid]['user_avg_stars']
        else:
            return user_avg
        
    similarity = sorted(similarity, key=lambda x: x[0], reverse=True)
    if len(similarity)>=5: 
        similarity =similarity[:5]
        
    
    numer=0
    denom=0
    
    for x in similarity:
        numer=numer+(x[0]*x[1])
        denom=denom+x[0]
        
    res=numer/denom
    
    return res
    
res = test_train_join.map(lambda x: (((x[0], x[1][0]), mapper(x,biz_left_out_user_avg)))).map(lambda x:(x[0][0],x[0][1],x[1])).collect()
res.extend(user_get_avg)
res.extend(anybody_left)
df = pd.DataFrame(res, columns=['user_id', 'business_id','cf_rating'])


X_train=biz2[['user_review_count','user_avg_stars', 'biz_review_count', 'biz_avg_stars']]
Y_train=biz2[['stars']]

test=pd.read_json(test_file_path, lines=True)

test=test.merge(user_data,how='left',on='user_id')
test['user_review_count']=test['user_review_count'].fillna(test['user_review_count'].mean())
test['user_avg_stars']=test['user_avg_stars'].fillna(test['user_avg_stars'].mean())

test=test.merge(biz_data,how='left',on='business_id')
test['biz_review_count']=test['biz_review_count'].fillna(test['biz_review_count'].mean())
test['biz_avg_stars']=test['biz_avg_stars'].fillna(test['biz_avg_stars'].mean())
X_test=test[['user_review_count','user_avg_stars', 'biz_review_count', 'biz_avg_stars']]
ids=test[['user_id','business_id']]
model = xgb.XGBRegressor(objective ='reg:linear')
model.fit(X_train, Y_train)
pred = model.predict(X_test)

ids=test[['user_id','business_id']]
ids['xgb_rating']=pred

final_res=df.merge(ids,how='right',on=['user_id','business_id'])
final_res['stars']=final_res['cf_rating']*0.2 + final_res['xgb_rating']*0.8
del final_res['cf_rating']
del final_res['xgb_rating']
tuples_ = [tuple(x) for x in final_res.values]
with open(output_file, "w+") as f:
    for x in tuples_:
        res_dic={"user_id":x[0],"business_id":x[1],"stars":x[2]}
        json.dump(res_dic, f)
        f.write('\n')
        
end=time.time()
print("Duration:",end-start)

