import time
import math
import itertools
import sys

from pyspark import SparkConf, SparkContext

sc=SparkContext()

filter_threshold=int(sys.argv[1])
support_=int(sys.argv[2])
input_file=sys.argv[3]
output_file=sys.argv[4]

support=support_


start=time.time()
def add(a,b):
    return a+b



df1=sc.textFile(input_file).persist()
header = df1.first()
df1=df1.filter(lambda x: x != header).map(lambda x: x.split(',')).map(lambda x: ((x[0]), (x[1]))).groupByKey().mapValues(set).map(lambda x: x[1]).filter(lambda x: len(x) >filter_threshold).persist()
total_baskets=len(df1.collect())

def freq_generator(k,partition_,threshold,frequent_itemsets,previous):
        
    candidates = []
    for i in range(len(previous)-1):
        for j in range(i+1, len(previous)):
            a = previous[i]
            b = previous[j]
            if a[0:(k-2)] == b[0:(k-2)]:
                candidates.append(tuple((set(a) | set(b))))
            else:
                break
    

    frequents={} 
    for x in candidates:
        for y in partition_:
            if set(x).issubset(y) and x not in frequents:
                frequents[x]=1
            elif set(x).issubset(y) and x in frequents:
                frequents[x]+=1
                
    frequents_final={}
    for k,v in frequents.items():
        if v >= threshold:
            frequents_final[k]=v
            
    
    frequents_finale=sorted(frequents_final)
    frequent_itemsets.extend(frequents_finale)
    
    return frequents_finale
   

def mapper1(iterator):
    
    partition=list(iterator)
    no_baskets_partition=len(partition)
    p_partition= no_baskets_partition/total_baskets
    support_partition= support * p_partition
    frequent_itemsets=[]
    
   
    dic1={}
    singletons={}
    
        
    for x in partition:
        for i in x:
            if i not in dic1:
                dic1[i]=1
            else:
                dic1[i]+=1
    for k, v in dic1.items():
        if v >= support_partition:
            singletons[k]=v
            
            
    singletons=sorted(singletons)  
    frequent_itemsets.extend(singletons)
    
    freq_singletons=set(singletons)
  
    

    
    pair=set(itertools.combinations(freq_singletons, 2))
    dic2={}
    pairs=[]
    for x in pair:
        #count=0
        for y in partition:
            if set(x).issubset(y) and x not in dic2:
                dic2[x]=1
            elif set(x).issubset(y) and x in dic2:
                dic2[x]+=1
    
    dic2_final={}
    for k,v in dic2.items():
        if v >= support_partition:
            dic2_final[k]=v
            
    dic2_finale=sorted(dic2_final)
    frequent_itemsets.extend(dic2_finale)
    
 

   
   
    curr_freq=dic2_finale
    k=3
    
    while len(curr_freq)!=0:
        
        curr_freq=freq_generator(k,partition,support_partition,frequent_itemsets,dic2_finale)
        dic2_finale=curr_freq
        k+=1
    
    
    yield frequent_itemsets



t=df1.mapPartitions(mapper1).flatMap(lambda x:x).distinct().collect() 


map1_op=[]
for i in t:
    if type(i) is tuple:
        map1_op.append(tuple(sorted(i)))
    else:
        map1_op.append(i)

map1_final=list(set(map1_op))


max_length_p1=0
freq_singles_p1=[]
for i in map1_final:
    if type(i) is tuple:
        max_length_p1=max(len(i),max_length_p1)
    else:
        freq_singles_p1.append(i)
        
        
final_res_p1=[]
length=2
while length <= max_length_p1:
    array=[]
    for i in map1_final:
        if type(i) is tuple and len(i)==length:
            array.append(i)
            array.sort()
    
    final_res_p1.append(array)
    length+=1
    
def mapper2(iterator):
    
    partition=list(iterator)
    chunk_counter=[]
    for x in map1_final:
        count=0
        for y in partition:
            if type(x) is tuple and set(x).issubset(y):
                    count+=1
            else:
                if x in y:
                    count+=1
        chunk_counter.append((x,count))
        
    yield chunk_counter
    
f=df1.mapPartitions(mapper2).flatMap(lambda x: x).reduceByKey(add).filter(lambda x:x[1] >= support).map(lambda x:x[0]).collect()

max_length=0
freq_singles=[]
for i in f:
    if type(i) is tuple:
        max_length=max(len(i),max_length)
    else:
        freq_singles.append(i)
        
final_res=[]
length=2
while length <= max_length:
    array=[]
    for i in f:
        if type(i) is tuple and len(i)==length:
            array.append(i)
            array.sort()
    
    final_res.append(array)
    length+=1
    
l1 = []
l2 = []
for i in freq_singles:
    l1.append(str(i))
    t = tuple(l1)
    l2.append(t)
    l2.sort()
    l1 = []
    
l3 = []
l4 = []
for i in freq_singles_p1:
    l3.append(str(i))
    t1 = tuple(l3)
    l4.append(t1)
    l4.sort()
    l3 = []
    

with open(output_file,'w') as f:
    f.write('Candidates:')
    f.write('\n')
    f.write(','.join('({})'.format("'"+t[0]+"'") for t in l4))
    f.write('\n\n')
    for x in final_res_p1:
        l="),".join(str(sorted(x))[1:-1].split("), "))
        f.write(l)
        f.write('\n\n')
    f.write('Frequent Itemsets:')
    f.write('\n')
    f.write(','.join('({})'.format("'"+t[0]+"'") for t in l2))
    f.write('\n\n')
    for x in final_res:
        l="),".join(str(sorted(x))[1:-1].split("), "))
        f.write(l)
        f.write('\n\n')


end=time.time()

print("Duration: ",end-start)




