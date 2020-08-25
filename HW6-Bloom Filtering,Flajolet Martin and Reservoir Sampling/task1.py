import json
import binascii
import math
import random
import time
import sys
from pyspark import SparkConf, SparkContext


sc=SparkContext()
sc.setLogLevel("ERROR")
start=time.time()

first_json=sys.argv[1]
second_json=sys.argv[2]
op_file=sys.argv[3]


city_mapping = sc.textFile(first_json).map(lambda x: (json.loads(x)['city'])).filter(lambda x:x!='').map(lambda x:int(binascii.hexlify(x.encode('utf8')),16)).distinct().collect()


m=len(city_mapping)
log_val=math.log(2)
n=int((5*m)/log_val)
bit_array=[]
for x in range(0,n):
    bit_array.append(0)
    
#city_mapping=city.map(lambda x:int(binascii.hexlify(x.encode('utf8')),16)).collect()

list1=[]
list2=[]
#list3=[53,97,193,389,769]
for x in range(1,101):
    list1.append(x)

for x in range(100,201):
    list2.append(x)
a=random.sample(list1,5)
b=random.sample(list2,5)
#c=random.sample(list3,5)

hash_variables=[]
for i,j in list(zip(a,b)):
    hash_variables.append((i,j))

for point in city_mapping:
    for x in hash_variables:
        a_=x[0]
        b_=x[1]
        #p=x[2]
        point_hashed=((a_*point)+b_)%len(bit_array)
        bit_array[point_hashed]=1

def mapper1(city,hash_list,bitarray):
    if city=='':
        return 0
    elif city is None:
        return 0
    else:
        city_to_map=int(binascii.hexlify(city.encode('utf8')),16)
        for x in hash_list:
            a_=x[0]
            b_=x[1]
            city_hashed=((a_*city_to_map)+b_)%len(bitarray)
            if bitarray[city_hashed]==0:
                return 0
        return 1
    
def cityfinder(dicti):
    if 'city' in dicti:
        return json.loads(dicti)['city']
    
      
city2 = sc.textFile(second_json).map(lambda x:cityfinder(x)).map(lambda x:mapper1(x,hash_variables,bit_array))

with open(op_file,'w') as f:
    for x in city2.collect():
        f.write(str(x)+' ')
        #f.write('')

end=time.time()

print('DURATION:',end-start)


