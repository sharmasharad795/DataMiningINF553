import json
import sys
import time
from pyspark import SparkConf, SparkContext

sc=SparkContext()

train_file=sys.argv[1]
test_file=sys.argv[2]
model_file=sys.argv[3]
output_file=sys.argv[4]
cf_type=sys.argv[5]


start=time.time()


if cf_type == "item_based":
    task3_model_mapping=sc.textFile(model_file).map(lambda x: ((json.loads(x)['b1'],json.loads(x)['b2']),json.loads(x)['sim'])).collectAsMap()
    test_file=sc.textFile(test_file).map(lambda x: (json.loads(x)['user_id'],json.loads(x)['business_id'])).persist()


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

    def mapper(x):

        userid,bizid,sim_business = x[0],x[1][0],x[1][1]
        similarity = []
        for j in sim_business:
            if(bizid,j) in task3_model_mapping: 
                similarity.append((task3_model_mapping[(bizid,j)],sim_business[j]))
            if (j,bizid) in task3_model_mapping:
                similarity.append((task3_model_mapping[(j,bizid)],sim_business[j]))
        if len(similarity)==0:
            return None
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
    
    res = test_train_join.map(lambda x: (((x[0], x[1][0]), mapper(x)))).filter(lambda x: x[1] is not None).collect()


    with open(output_file, "w+") as f:
        for x in res:
            res_dic={"user_id":x[0][0],"business_id":x[0][1],"stars":x[1]}
            json.dump(res_dic, f)
            f.write('\n')

    end=time.time()
    print("Duration:",end-start)
    
else:
   
    model_file=sc.textFile(model_file).map(lambda x: ((json.loads(x)['u1'],json.loads(x)['u2']),json.loads(x)['sim'])).collectAsMap()
    test_file=sc.textFile(test_file).map(lambda x: (json.loads(x)['user_id'],json.loads(x)['business_id'])).map(lambda x:(x[1],x[0])).persist()


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

    t_file=sc.textFile(train_file).persist()
    train_file=t_file.map(lambda x: (json.loads(x)['business_id'],(json.loads(x)['user_id'],json.loads(x)['stars']))).groupByKey().mapValues(list).map(lambda x:(x[0],aggregator(x[1]))).persist()              
    usr_avg=t_file.map(lambda x: (json.loads(x)['user_id'],json.loads(x)['stars'])).groupByKey().mapValues(list).map(lambda x:(x[0],(sum(x[1]),len(x[1])))).collectAsMap()                 

    def mapper(x):

        userid,bizid,sim_business = x[0],x[1][0],x[1][1]
        similarity = []
    
        numer=0
        denom=0
        for j in sim_business:
            if(bizid,j) in model_file:
                j_avg=(usr_avg[j][0]-sim_business[j])/(usr_avg[j][1]-1)
                numer=numer+((sim_business[j]-j_avg)*model_file[(bizid,j)])
                denom=denom+model_file[(bizid,j)]
            if (j,bizid) in model_file:
                j_avg=(usr_avg[j][0]-sim_business[j])/(usr_avg[j][1]-1)
                numer=numer+((sim_business[j]-j_avg)*model_file[(j,bizid)])
                denom=denom+model_file[(j,bizid)]
         
        if denom==0:
            return None
        
        if numer==0:
            return usr_avg[bizid][0]/usr_avg[bizid][1]
    
        else:
            ans=(usr_avg[bizid][0]/usr_avg[bizid][1]) + (numer/denom)
            if ans>5:
                return None
            else:
                return ans
    
    test_train_join = test_file.join(train_file).map(lambda x: (((x[1][0], x[0]), mapper(x)))).filter(lambda x: x[1] is not None).collect()

    with open(output_file, "w+") as f:
        for x in test_train_join:
            res_dic={"user_id":x[0][0],"business_id":x[0][1],"stars":x[1]}
            json.dump(res_dic, f)
            f.write('\n')

    end=time.time()
    print('Duration:',end-start)



