import os
import pyspark
from pyspark import SparkContext,SparkConf
import sys
import time
import random
import math
import csv
import json

os.environ['PYSPARK_PYTHON']='/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON']='/usr/local/bin/python3.6'

sc=SparkContext()
sc.setLogLevel("ERROR")
start=time.time()
path=sys.argv[1]
cluster=int(sys.argv[2])
op_file1=sys.argv[3]
op_file2=sys.argv[4]


##convert float to int while printing in last step to file

#this 
final_tracker=[]

###this list stores results to be written into the intermediate.csv
intermediate_stats=[]

allfiles=[]


for x,y,z in os.walk(path):
    for a in z:
        allfiles.append(os.path.join(x,a))


###function to find euclidean dist, used in mapper 2
def Euclid_dist(a,b):
    euclid_dist = math.sqrt(sum([(i - j) ** 2 for i, j in zip(a, b)]))
    return euclid_dist

###function to find the point and its closest centroid, tuple of (closestcentroid,point)-in their full dims
def mapper2(element,map_,initial_dscenter_val_):
    element_dim=map_[element]
    euc_dist=[]
    for x in initial_dscenter_val_:
        edist=Euclid_dist(element_dim,x)
        euc_dist.append((x,edist))
    v=min(euc_dist, key = lambda t: t[1])[0]
        
    return((v,element_dim))

##calculating the new centroid
def mapper3(tup_list):
    div=len(tup_list)
    ans=tuple([sum(x) for x in zip(*tup_list)])
    ans_=tuple(map(lambda x:x/div,ans))
    return ans_

###getting the stats for SUM,SUMSQ,N for each cluster
def mapper4(tuple_list):
    N=len(tuple_list)
    sum_=tuple([sum(x) for x in zip(*tuple_list)])
    new=[]
    for a in tuple_list:
        new.append(tuple([x**2 for x in a]))
    sumsq=tuple([sum(x) for x in zip(*new)])
    stats=[]
    stats.append((sum_,sumsq,N))
    return stats

### returning row ids of points from their dimensions
def mapper5(hash_list,dic_):
    new_hash_list=[]
    for x in hash_list:
        new_hash_list.append(dic_[x])
        
    return new_hash_list
      
def kmeans(k_list,initial_dscenter_,tol):
    k_list=sc.parallelize(k_list).map(lambda x:(x[0],x[1:])).persist()
    k_list_map=k_list.collectAsMap()
    reverse_map={v: k for k, v in k_list_map.items()}

    ###get random centroids
    probable_centers=list(k_list_map.keys())
    #initial_dscenter=random.sample(probable_centers,initial_dscenter_)
    #print(initial_dscenter)
    centroid=[]
    centroid.extend(random.sample(probable_centers,1))
    
    
    for x in range(initial_dscenter_ - 1):
        distances=[]
        for y in probable_centers:
            point_dim_y=k_list_map[y]
            temp_min = sys.maxsize
            for z in centroid: 
                point_dim_centroid=k_list_map[z]
                #print(point_dim_centroid)
                eu_dist = Euclid_dist(point_dim_y, point_dim_centroid) 
                temp_min = min(temp_min, eu_dist) 
            distances.append((temp_min,y))
        to_centroid = max(distances,key=lambda x:x[0])[1]
        centroid.append(to_centroid) 
        distances = [] 
    
    initial_dscenter_val=[]
    for a in centroid:
        initial_dscenter_val.append(k_list_map[a])
    
   
    num_iters=0
    while num_iters<100:
        #####output of mapper 3 contains both old and new centroid
        ds_centers_later_oldnew=k_list.map(lambda x:x[0]).map(lambda x:mapper2(x,k_list_map,initial_dscenter_val)).groupByKey().mapValues(list).map(lambda x:(x[0],(mapper3(x[1]),x[1]))).persist()
        ds_centers_later=ds_centers_later_oldnew.map(lambda x:(x[1][0],x[1][1])).persist()
        ds_centers_old=ds_centers_later_oldnew.map(lambda x:x[0]).collect()
        ds_centers_new=ds_centers_later_oldnew.map(lambda x:x[1][0]).collect()
        
        ds_centers=ds_centers_later.collect()
        initial_dscenter_val=list(map(lambda x:x[0],ds_centers))
        
        get_out=True
        for i in range(0,len(ds_centers_new)):
            cur=ds_centers_new[i]
            older=ds_centers_old[i]
            diff=sum([(i - j)/(j*100) for i, j in zip(cur, older)])
            if diff>tol:
                get_out=False
                #break
             
        if get_out:
            break
        
        num_iters=num_iters+1
        #print(num_iters)
        
    result=ds_centers_later.map(lambda x:(x[1],mapper4(x[1]))).map(lambda x:(mapper5(x[0],reverse_map),x[1])).collect()

    return result
      
    
file1=allfiles[0]

rounds=1
k_list=[]    
with open(file1) as f:
    for line in f:
        k_list.append(tuple([float(n) for n in line.strip().split(',')]))
        
k_list_map=sc.parallelize(k_list).map(lambda x:(x[0],x[1:])).collectAsMap()
test_kmeans_outliers=kmeans(k_list,2*cluster,0.5)

rs=[]
for x in test_kmeans_outliers:
    if len(x[0])==1:
        rs.extend(x[0])
        
outliers=[]
for x in rs:
    if x in k_list_map:
        temp_tuple=(x,)+k_list_map[x]
        outliers.append(temp_tuple)

k_list=list(set(k_list)-set(outliers))
ds_data=random.sample(k_list,math.ceil(len(k_list)*0.7))
csrs_data=list(set(k_list)-set(ds_data))

###contains points per ds and per ds, its stats            
ds1_exp=kmeans(ds_data,cluster,0.5)

ds_center_r1=[]

param3=0
for x in range(0,len(ds1_exp)):
    final_tracker.append((x,ds1_exp[x][0]))
    ds_center_r1.append(ds1_exp[x][1])
    param3=param3+len(ds1_exp[x][0])

  
    
ds_center_stats=[]
for x in range(0,len(ds_center_r1)):
    ds_center_stats.append((x,ds_center_r1[x]))
    
###ds_center_stats is a list of stats per ds cluster, in the form of a tuple of cluster no and its stats in a list
    
csrs_exp=kmeans(csrs_data,3*cluster,4)
cs=[]


##rs only contains the points
##cs contains both points and stats of each cs cluster
for x in csrs_exp:
    if len(x[0])==1:
        rs.extend(x[0])
    else:
        cs.append(x)
        
###rs dimension mapping of points
point_rs_map=[]
for x in rs:
    if x in k_list_map:
        temp_tuple=(x,)+k_list_map[x]
        point_rs_map.append(temp_tuple)


cs_center_points=[]

##cs_center_stats maintain stats per cs cluster in the form a list of tuples
cs_center_stats=[]
for x in range(0,len(cs)):
    cs_center_points.append((x,cs[x][0]))    
    cs_center_stats.append((x,cs[x][1]))

    
##this map contains the points per cs cluster
cs_center_points_map=sc.parallelize(cs_center_points).collectAsMap()
    
param1=rounds
param2=len(ds1_exp)
param4=len(cs)
param6=len(rs)

param5=0

for x in cs:
    param5=param5+len(x[0])
    
intermediate_stats.append((param1,param2,param3,param4,param5,param6))
###keeps track of points in DS cluster
final_tracker_map=sc.parallelize(final_tracker).collectAsMap()







#############SECOND PHASE###################


def map_todim(listpoints,map_):
    new_list=[]
    for x in listpoints:
        new_list.append(map_[x])
    return new_list


###get centroid and std per DS cluster
def getstd(getlist):
    
    centroid=tuple(map(lambda x: x/getlist[0][2], getlist[0][0]))
    sumsq_divN=tuple(map(lambda x: x/getlist[0][2], getlist[0][1]))
    sq_sum=tuple(map(lambda x: x ** 2, centroid))
    variance=tuple([(i - j) for i, j in zip(sumsq_divN,sq_sum)])
    std=tuple(map(lambda x:math.sqrt(x),variance))
    return (centroid,std)

###RETURNS the index of cluster where the min mahalobnis dist was found and the dist as well
def point_ds_calculation(point,map_pt,map_ds,param,center):
    pt_dimensions=map_pt[point]
    append_list=[]
    for key,value in map_ds.items():
        centroid_x=value[0]
        std_x=value[1]
        numer = tuple([(i - j) for i, j in zip(pt_dimensions,centroid_x)])
        if 0 not in std_x:
            numer_denom=tuple([(i/j) for i, j in zip(numer,std_x)])
        else:
            numer_denom=numer
        numer_denom_squared=tuple(map(lambda x: x ** 2, numer_denom))
        numer_denom_squared_add_sqrt=math.sqrt(sum(numer_denom_squared))
        append_list.append(numer_denom_squared_add_sqrt)
    
    
    
    final_res_list=[]
    for x in append_list:
        if x<param:
            final_res_list.append(x)
        else:
            final_res_list.append(param)
         
    if (any(x < param for x in final_res_list)):
        return (center[final_res_list.index(min(final_res_list))][0],min(final_res_list))
    else:
        return (-1,-1)


for x in range(1,len(allfiles)):
#for x in range(9,10):
    rounds=rounds+1
    file_cons=allfiles[x]
    
    sec_list=[]    
    with open(file_cons) as f:
        for line in f:
            sec_list.append(tuple([float(n) for n in line.strip().split(',')]))
       
    sec_list=sc.parallelize(sec_list).map(lambda x:(x[0],x[1:])).persist()
    sec_list_map=sec_list.collectAsMap()
    d=len(list(sec_list_map.values())[0])


    sqrootd=math.sqrt(d)


##sec_comp contains the point ids only
    sec_comp=sec_list.map(lambda x:x[0])

    ds_stats_map=sc.parallelize(ds_center_stats).persist() ### might use this as a map
###ds_stats_centroid_var_map gives the cluster no and its centroid,std for each DS
    ds_stats_centroid_var_map=ds_stats_map.map(lambda x:(x[0],getstd(x[1]))).collectAsMap()

##get the point and the cluster to which it goes using Maha dist
    points_in_ds_withcluster=sec_comp.map(lambda x:(x,point_ds_calculation(x,sec_list_map,ds_stats_centroid_var_map,2*sqrootd,ds_center_stats))).filter(lambda x:x[1][1]!=-1).map(lambda x:(x[1][0],x[0])).persist()     
##get list of points which went to DS
    points_in_ds=points_in_ds_withcluster.map(lambda x:x[1]).persist()

    ds_latest_stats=points_in_ds_withcluster.groupByKey().mapValues(list).map(lambda x:(x[0],map_todim(x[1],sec_list_map))).map(lambda x:(x[0],mapper4(x[1]))).collectAsMap()

    temp_ds_center_stats=[]
    for x in ds_center_stats:
        if x[0] in ds_latest_stats:
            new_sum=ds_latest_stats[x[0]][0][0]
            new_sumsq=ds_latest_stats[x[0]][0][1]
            new_N=ds_latest_stats[x[0]][0][2]
            old_sum=x[1][0][0]
            old_sumsq=x[1][0][1]
            old_N=x[1][0][2]
        
            N_=new_N+old_N
            sum_=tuple([(i + j) for i, j in zip(old_sum,new_sum)])
            sum_sq_=tuple([(i + j) for i, j in zip(old_sumsq,new_sumsq)])
            qs=[]
            qs.append((sum_,sum_sq_,N_))
            temp_ds_center_stats.append((x[0],qs))
        else:
            temp_ds_center_stats.append(x)

##updating ds stats
    ds_center_stats=temp_ds_center_stats
       


###cs_stats_map and ds_stats_centroid_var_map perform the same function for CS and DS clusters


    probable_points_in_cs=sec_comp.subtract(points_in_ds)
    cs_stats_map=sc.parallelize(cs_center_stats).map(lambda x:(x[0],getstd(x[1]))).collectAsMap()
    points_in_cs_withcluster=probable_points_in_cs.map(lambda x:(x,point_ds_calculation(x,sec_list_map,cs_stats_map,2*sqrootd,cs_center_stats))).filter(lambda x:x[1][1]!=-1).map(lambda x:(x[1][0],x[0])).persist()     

    points_in_cs=points_in_cs_withcluster.map(lambda x:x[1]).persist()
    cs_latest_stats=points_in_cs_withcluster.groupByKey().mapValues(list).map(lambda x:(x[0],map_todim(x[1],sec_list_map))).map(lambda x:(x[0],mapper4(x[1]))).collectAsMap()

    temp_cs_center_stats=[]
    for x in cs_center_stats:
        if x[0] in cs_latest_stats:
            new_sum=cs_latest_stats[x[0]][0][0]
            new_sumsq=cs_latest_stats[x[0]][0][1]
            new_N=cs_latest_stats[x[0]][0][2]
            old_sum=x[1][0][0]
            old_sumsq=x[1][0][1]
            old_N=x[1][0][2]
        
            N_=new_N+old_N
            sum_=tuple([(i + j) for i, j in zip(old_sum,new_sum)])
            sum_sq_=tuple([(i + j) for i, j in zip(old_sumsq,new_sumsq)])
            qs=[]
            qs.append((sum_,sum_sq_,N_))
            temp_cs_center_stats.append((x[0],qs))
        else:
            temp_cs_center_stats.append(x)

###updating cs stats
    cs_center_stats=temp_cs_center_stats

##adding cs point to CS map of cluster:[list of points]
    for x in points_in_cs_withcluster.collect():
        if x[0] in cs_center_points_map:
            cs_center_points_map[x[0]].append(x[1])
        

    points_in_rs=probable_points_in_cs.subtract(points_in_cs).persist()
###dic for row id to dimensions


    points_in_rs_dimdata=[]
    for x in points_in_rs.collect():
        if x in sec_list_map:
            temp_tuple=(x,)+sec_list_map[x]
            points_in_rs_dimdata.append(temp_tuple)

    point_rs_map.extend(points_in_rs_dimdata)
    point_rs_mapper=sc.parallelize(point_rs_map).map(lambda x:(x[0],x[1:])).collectAsMap()


##kmeans on rs points
    if len(point_rs_map)>=((3*cluster)+1):
        rs_kmeans=kmeans(point_rs_map,3*cluster,4)
        cs1=[]
        rs1=[]
        for x in rs_kmeans:
            if len(x[0])==1:
                rs1.extend(x[0])
            else:
                cs1.append(x)
        point_rs_map=[]
        for x in rs1:
            temp_tuple=(x,)+point_rs_mapper[x]
            point_rs_map.append(temp_tuple)
        cluster_no=max(cs_center_points_map, key=int)+1
        for x in cs1:
            cluster_key=cluster_no
            cluster_val=x[0]
            cs_center_points_map[cluster_key]=cluster_val
            cs_center_stats.append((cluster_key,x[1]))
            cluster_no=cluster_no+1
        
    cs_merge_list=[]
###merging cs clusters#########
    
    for x in range(0,len(cs_center_stats)):
        point_small_centroid,point_small_std=getstd(cs_center_stats[x][1])
        min_dist=2*sqrootd
        small_cluster=cs_center_stats[x]
        present_maha=True 
        #print(point_small_centroid)
        for y in range(x+1,len(cs_center_stats)):
            if cs_center_stats[x][1][0][2]<cs_center_stats[y][1][0][2]:
                sd=cs_center_stats[y][1]
                big_cluster_centroid,big_cluster_std=getstd(sd)
                #print(big_cluster_centroid)
                numer = tuple([(i - j) for i, j in zip(point_small_centroid,big_cluster_centroid)])
                if 0 not in big_cluster_std:
                    numer_denom=tuple([(i/j) for i, j in zip(numer,big_cluster_std)])
                else:
                    numer_denom=numer
                numer_denom_squared=tuple(map(lambda x: x ** 2, numer_denom))
                numer_denom_squared_add_sqrt=math.sqrt(sum(numer_denom_squared))
                       
                if numer_denom_squared_add_sqrt < min_dist:
                    present_maha=False
                    min_dist=numer_denom_squared_add_sqrt
                    big_cluster_tomergeinto=cs_center_stats[y]
        #print(present_maha)
 
        if present_maha==False:
            cs_merge_list.append((small_cluster,big_cluster_tomergeinto))
        
    small_c=[]
    big_c=[]
    for x in cs_merge_list:
        small_c.append(x[0][0])
        big_c.append(x[1][0])


    done_list_big=[]
    for x in cs_merge_list:
        if x[1][0] not in small_c and x[1][0] not in done_list_big:
            big_cluster=x[1]
            done_list_big.append(x[1][0])
            small_cluster=x[0]
            #print(small_cluster[0],big_cluster[0])
            new_cluster_id=big_cluster[0]
            new_cluster_N=big_cluster[1][0][2]+small_cluster[1][0][2]
            new_cluser_sum=tuple([(i + j) for i, j in zip(small_cluster[1][0][0],big_cluster[1][0][0])])
            new_cluser_sumsq=tuple([(i + j) for i, j in zip(small_cluster[1][0][1],big_cluster[1][0][1])])
            new_trio=(new_cluster_id,[tuple((new_cluser_sum,new_cluser_sumsq,new_cluster_N))])
            cs_center_stats = [i for i in cs_center_stats if i[0]!=small_cluster[0] and i[0]!=big_cluster[0]]
            #print(len(cs_center_stats))
            cs_center_stats.append(new_trio)
            #print(len(cs_center_stats))
            cs_center_points_map[big_cluster[0]].extend(cs_center_points_map[small_cluster[0]])
            del cs_center_points_map[small_cluster[0]]
        


    

####adding the new DS points to final_tracker_map    

    for x in points_in_ds_withcluster.collect():
        if x[0] in final_tracker_map:
            final_tracker_map[x[0]].append(x[1])
        
    
    ###last round merge cs with ds
    
    if rounds==len(allfiles):
       
        ds_center_stats_map=sc.parallelize(ds_center_stats).map(lambda x:(x[0],(x[1][0][0],x[1][0][1],x[1][0][2]))).collectAsMap()

        last_round_csds_merger=[]
        for x in range(0,len(cs_center_stats)):
            point_small_centroid,point_small_std=getstd(cs_center_stats[x][1])
            append_list=[]
            for key,value in ds_center_stats_map.items():
                centroid_x=value[0]
                std_x=value[1]
                numer = tuple([(i - j) for i, j in zip(point_small_centroid,centroid_x)])
                if 0 not in std_x:
                    numer_denom=tuple([(i/j) for i, j in zip(numer,std_x)])
                else:
                    numer_denom=numer
                numer_denom_squared=tuple(map(lambda i: i ** 2, numer_denom))
                numer_denom_squared_add_sqrt=math.sqrt(sum(numer_denom_squared))
                append_list.append(numer_denom_squared_add_sqrt)
        
            final_res_list=[]
            for a in append_list:
                if a<(2*sqrootd):
                    final_res_list.append(a)
                else:
                    final_res_list.append(2*sqrootd)
            #print(final_res_list)
            if (any(b < (2*sqrootd) for b in final_res_list)):
                last_round_csds_merger.append((cs_center_stats[x][0],ds_center_stats[final_res_list.index(min(final_res_list))][0],min(final_res_list)))
     
        for x in last_round_csds_merger:
            #print(x[0])
            final_tracker_map[x[1]].extend(cs_center_points_map[x[0]])
            del cs_center_points_map[x[0]]
            
        ###get intermediate stats
        param1=rounds
        param2=len(final_tracker_map)

        param3=0
        for k,v in final_tracker_map.items():
            param3=param3+len(v)
    
        param4=len(cs_center_points_map)

        param5=0
        for k,v in cs_center_points_map.items():
            param5=param5+len(v)
  
    
        param6=len(point_rs_map)
    
        intermediate_stats.append((param1,param2,param3,param4,param5,param6))
        
        points_outliers=[]
        for x in point_rs_map:
            #print(x)
            points_outliers.append(x[0])
    
        for k,v in cs_center_points_map.items():
            for vi in v:
                points_outliers.append(vi)
        
        final_tracker_map[-1]=points_outliers
            
        
    else:
        ###get intermediate stats
        param1=rounds
        param2=len(final_tracker_map)

        param3=0
        for k,v in final_tracker_map.items():
            param3=param3+len(v)
    
        param4=len(cs_center_points_map)

        param5=0
        for k,v in cs_center_points_map.items():
            param5=param5+len(v)
  
    
        param6=len(point_rs_map)
    
        intermediate_stats.append((param1,param2,param3,param4,param5,param6))

cols = ['round_id', 'nof_cluster_discard', 'nof_point_discard', 'nof_cluster_compression', 'nof_point_compression',
          'nof_point_retained']
with open(op_file2, 'w') as f1:
    csvwriter = csv.writer(f1)
    csvwriter.writerow(cols)
    csvwriter.writerows(intermediate_stats)
    
final_res=[]
for k,v in final_tracker_map.items():
    for vi in v:
        final_res.append((str(int(vi)),k))
        
final_res_dic=dict(final_res)  
with open(op_file1, 'w') as f2:
    json.dump(final_res_dic, f2)
    


end=time.time()
print('Duration:',end-start)
       