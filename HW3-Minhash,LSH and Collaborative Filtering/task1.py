import json
import re
import sys
from pyspark import SparkConf, SparkContext
import csv
from collections import defaultdict
import itertools
from itertools import combinations
import time

sc=SparkContext()
sc.setLogLevel("ERROR")

input_file=sys.argv[1]
output_file=sys.argv[2]

start=time.time()
t1 = sc.textFile(input_file).map(lambda x: (json.loads(x)['business_id'],json.loads(x)['user_id'])).persist()
#main_t1=t1.collect()

users=t1.map(lambda x : x[1]).distinct().collect()
#business=t1.map(lambda x : x[0]).distinct()

user_dic={}
count=0
for x in users:
    user_dic[x]=count
    count+=1

t1=t1.mapValues(lambda x:user_dic[x])
#bucket_size=len(users)
bucket_size=49157

t1map = t1.groupByKey().map(lambda x:(x[0],list(set(x[1])))).collectAsMap()

def hashfn(a,b):
    hashf=t1.map(lambda x:(x[0],((a*x[1]+b)%bucket_size))).groupByKey().map(lambda x:(x[0],sorted(list(x[1])))).map(lambda x:(x[0],x[1][0])).persist()
    return hashf


hash1=hashfn(123,14)
hash2=hashfn(5,124)
hash3=hashfn(14,139)
hash4=hashfn(210,74)
hash5=hashfn(176,98)
hash6=hashfn(6,100)
hash7=hashfn(167,24)
hash8=hashfn(63,188)
hash9=hashfn(49,154)
hash10=hashfn(100,23)
hash11=hashfn(95,125)
hash12=hashfn(178,29)
hash13=hashfn(31,130)
hash14=hashfn(148,41)
hash15=hashfn(3,197)
hash16=hashfn(176,76)
hash17=hashfn(11,122)
hash18=hashfn(4,134)
hash19=hashfn(16,157)
hash20=hashfn(204,79)
hash21=hashfn(178,91)
hash22=hashfn(8,101)
hash23=hashfn(168,25)
hash24=hashfn(43,158)
hash25=hashfn(29,184)
hash26=hashfn(200,63)
hash27=hashfn(75,144)
hash28=hashfn(172,99)
hash29=hashfn(51,150)
hash30=hashfn(118,47)
hash31=hashfn(5,207)
hash32=hashfn(149,56)
hash33=hashfn(9,11)
hash34=hashfn(26,11)
hash35=hashfn(3,20)



sig_matrix = hash1.join(hash2).map(lambda x:(x[0],list(x[1])))

def sorter(a):
    return a.sort()



def joiner(a, b):
    a.append(b)
    return a

def matrix_creator(sig_matrix,fn):
    #return sig_matrix.join(fn).map(lambda x:(x[0],list(x[1]))).map(lambda x : (x[0], joiner(x[1][0], x[1][1])))  
    return sig_matrix.join(fn).mapValues(list).map(lambda x : (x[0], joiner(x[1][0], x[1][1])))


sig_matrix=matrix_creator(sig_matrix,hash3)
sig_matrix=matrix_creator(sig_matrix,hash4)
sig_matrix=matrix_creator(sig_matrix,hash5)
sig_matrix=matrix_creator(sig_matrix,hash6)
sig_matrix=matrix_creator(sig_matrix,hash7)
sig_matrix=matrix_creator(sig_matrix,hash8)
sig_matrix=matrix_creator(sig_matrix,hash9)
sig_matrix=matrix_creator(sig_matrix,hash10)
sig_matrix=matrix_creator(sig_matrix,hash11)
sig_matrix=matrix_creator(sig_matrix,hash12)
sig_matrix=matrix_creator(sig_matrix,hash13)
sig_matrix=matrix_creator(sig_matrix,hash14)
sig_matrix=matrix_creator(sig_matrix,hash15)
sig_matrix=matrix_creator(sig_matrix,hash16)
sig_matrix=matrix_creator(sig_matrix,hash17)
sig_matrix=matrix_creator(sig_matrix,hash18)
sig_matrix=matrix_creator(sig_matrix,hash19)
sig_matrix=matrix_creator(sig_matrix,hash20)
sig_matrix=matrix_creator(sig_matrix,hash21)
sig_matrix=matrix_creator(sig_matrix,hash22)
sig_matrix=matrix_creator(sig_matrix,hash23)
sig_matrix=matrix_creator(sig_matrix,hash24)
sig_matrix=matrix_creator(sig_matrix,hash25)
sig_matrix=matrix_creator(sig_matrix,hash26)
sig_matrix=matrix_creator(sig_matrix,hash27)
sig_matrix=matrix_creator(sig_matrix,hash28)
sig_matrix=matrix_creator(sig_matrix,hash29)
sig_matrix=matrix_creator(sig_matrix,hash30)
sig_matrix=matrix_creator(sig_matrix,hash31)
sig_matrix=matrix_creator(sig_matrix,hash32)
sig_matrix=matrix_creator(sig_matrix,hash33)
sig_matrix=matrix_creator(sig_matrix,hash34)
sig_matrix=matrix_creator(sig_matrix,hash35)




sig_list=sig_matrix.collect()


def find_candidates(a,b):
    f={}
    candidates=[]
    for i in sig_list:
        f[i[0]]=hash(tuple(i[1][a:b]))
    
    new_dict = defaultdict(list)
    for k, v in f.items():
        new_dict[v].append(k)

    for k,v in new_dict.items():
        if len(v)>1:
            candidates.append(v)
            
    return candidates


c1=find_candidates(0,1)
c2=find_candidates(1,2)
c3=find_candidates(2,3)
c4=find_candidates(3,4)
c5=find_candidates(4,5)
c6=find_candidates(5,6)
c7=find_candidates(6,7)
c8=find_candidates(7,8)
c9=find_candidates(8,9)
c10=find_candidates(9,10)
c11=find_candidates(10,11)
c12=find_candidates(11,12)
c13=find_candidates(12,13)
c14=find_candidates(13,14)
c15=find_candidates(14,15)
c16=find_candidates(15,16)

c17=find_candidates(16,17)
c18=find_candidates(17,18)
c19=find_candidates(18,19)
c20=find_candidates(19,20)
c21=find_candidates(20,21)
c22=find_candidates(21,22)
c23=find_candidates(22,23)
c24=find_candidates(23,24)
c25=find_candidates(24,25)
c26=find_candidates(25,26)
c27=find_candidates(26,27)
c28=find_candidates(27,28)
c29=find_candidates(28,29)
c30=find_candidates(29,30)
c31=find_candidates(30,31)
c32=find_candidates(31,32)
c33=find_candidates(32,33)
c34=find_candidates(33,34)
c35=find_candidates(34,35)




d=c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 +c13 + c14 + c15 + c16 +c17 + c18 + c19 + c20 + c21 + c22 + c23 + c24 + c25 + c26 + c27 + c28 +c29 + c30 +c1 +c32 + c33 +c34 +c35 
d.sort()
candidate_list=list(d for d,_ in itertools.groupby(d))

candidate_pairs=sc.parallelize(candidate_list).flatMap(lambda x : list(combinations(x, 2))).persist()


def jc(a,b):
    usera=set(t1map[a])
    userb=set(t1map[b])
    numerator=len(usera.intersection(userb))
    denominator=len(usera.union(userb))
    
    return numerator/denominator


real_pairs=candidate_pairs.map(lambda x:(x,(jc(x[0],x[1])))).filter(lambda x:x[1]>=0.05)

k=real_pairs.collect()
#print(k)

with open(output_file, "w+") as f:
   
    for x in k:
        res_dic={"b1":x[0][0],"b2":x[0][1],"sim":x[1]}
        json.dump(res_dic, f)
        f.write('\n')

end=time.time()
print('Duration:',end-start)

