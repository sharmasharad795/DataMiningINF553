import json
import sys
import time
import string
from collections import Counter
import math 
import re
from pyspark import SparkConf, SparkContext

#con=SparkConf.setAll[('spark.driver.memory','4g')]
#SparkContext.setSystemProperty('spark.driver.memory','4g')

sc=SparkContext()

input_file=sys.argv[1]
output_file=sys.argv[2]
stopwords=sys.argv[3]

start=time.time()

stopWords = sc.textFile(stopwords)

stopWords=stopWords.collect()

def stringer(s):
    s = s.lower()
    s = re.sub(r'[^A-Za-z]',' ',s).strip()
    swords = s.split()
    resultwords  = [word for word in swords if word not in stopWords]
    s = ' '.join(resultwords)
    return s

mp=sc.textFile(input_file).persist()

bp=mp.map(lambda x: (json.loads(x)['business_id'],json.loads(x)['text'])).reduceByKey(lambda a,b:a+b).mapValues(stringer).persist()
total_docs=bp.count()
#uniques_word=bp.flatMap(lambda x: x[1].split(' '))

#######trial for words to int #######

words=bp.map(lambda x:x[1]).flatMap(lambda x: x.split(' ')).distinct().collect()
words_dic={}
count=0
for x in words:
    words_dic[x]=count
    count+=1
    
def intmapper(wlist):
    intmapper=[]
    for i in wlist:
        intmapper.append(words_dic[i])
    return intmapper

bp=bp.mapValues(lambda x:x.split(' ')).mapValues(intmapper).persist()

#unique_word=bp.map(lambda x: len(x[1])).sum()#.split(' '))
#####################################

x=bp.map(lambda x: len(x[1])).sum()#.count()
rare_word_limit = 0.000001*x
words_lowcount=bp.flatMap(lambda x:x[1]).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).filter(lambda x: x[1]<rare_word_limit).collectAsMap()

def stringer2(s):
    #swords = s.split()
    resultwords  = [word for word in s if word not in words_lowcount]
    #s = ' '.join(resultwords)
    return resultwords
    
bp2=bp.mapValues(stringer2).persist()



def idf(i):
    return math.log2(total_docs/i)

idf=bp2.map(lambda x:(x[0],set(x[1]))).flatMap(lambda x:(x[1])).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b).mapValues(idf).collectAsMap()

def tf(listofwords):
    scn=Counter(listofwords)
    high = scn.most_common(1)
    for k,v in scn.items():
        scn[k]=scn[k]/high[0][1]
        scn[k]=scn[k]*idf[k]
    scn=sorted(scn.items(), key=lambda x: x[1], reverse=True)
    
    final_words=[]
    for x in scn[:200]:
        final_words.append(x[0])
    return final_words

bvector=bp2.mapValues(tf).collectAsMap()

def getup(bizlist):
    up_vector=[]
    for x in bizlist:
        up_vector.extend(bvector[x])
            
    return list(set(up_vector))

print('###################################################')
up=mp.map(lambda x: (json.loads(x)['user_id'],json.loads(x)['business_id'])).groupByKey().mapValues(list).mapValues(getup).collectAsMap()


#res={}
#res['business_vector']=bvector
#res['user_vector']=up

#with open(output_file,'w+') as f:
#    json.dump(res,f)
    
res = [bvector,up]
with open(output_file,'w',newline='')as f:
    #f.write('whatsup nigs')
    for r in res:
        json.dump(r,f)
        f.write('\n') 
              
             
end=time.time()
print('Duration:',end-start)