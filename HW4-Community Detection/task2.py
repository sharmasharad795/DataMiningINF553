import os
import pyspark
from pyspark import SparkContext,SparkConf
import sys
from itertools import combinations
from collections import defaultdict
import time

sc=SparkContext()
sc.setLogLevel("ERROR")
start=time.time()
threshold = int(sys.argv[1])
input_file = sys.argv[2]
bw_op_file = sys.argv[3]
com_op_file=sys.argv[4]

df1=sc.textFile(input_file).persist()
header = df1.first()
df1=df1.filter(lambda x: x != header).map(lambda x: (str(x.split(',')[0]), str(x.split(',')[1]))).persist()
user_biz_map=df1.groupByKey().mapValues(list).collectAsMap()

users=user_biz_map.keys()
pair_nodes=list(combinations(users,2))

ans=[]
for i in pair_nodes:
    biz1=user_biz_map[i[0]]
    biz2=user_biz_map[i[1]]
    inter = len(set(biz1)&set(biz2))
    if inter>=threshold:
        ans.append(((i[0],i[1]),inter))
        ans.append(((i[1],i[0]),inter))
    
candidate_nodes=sc.parallelize(ans).map(lambda x:(x[0][0],x[0][1])).persist()

adj_max=candidate_nodes.groupByKey().mapValues(list).collectAsMap()
bfs_nodes=adj_max.keys()

edge_dic={}

def betweeness(edge_dic,vertex):
    
    lev=0
    explored=[vertex]
    queue=[]
    bfs=[]
    queue.append(vertex)
    bfs_level_nodes={}
    
    while queue:
        lev=lev+1
        temp_list1=[]
        temp_list2=[]
        
        for x in queue:
            temp_list1.append(x)
            temp_list2.append((x,lev))
        bfs_level_nodes[lev]=temp_list1
        bfs.append(temp_list2)
        
        for i in range(0,len(queue)):
            current=queue.pop(0)
            neighbors=adj_max[current]
            for j in neighbors:
                if j not in explored:
                    explored.append(j)
                    queue.append(j)
                    
    bfs = [a for slist in bfs for a in slist]
    bfs=dict(bfs)
   
    
    level_sorted=sorted(bfs_level_nodes.keys(),reverse=True)

    node_dic={}
    


    for a in range(0,len(level_sorted)-1):
    
        nodes_at_level=bfs_level_nodes[level_sorted[a]]
    
        for x in nodes_at_level:
            if x not in node_dic:
                node_dic[x]=1
            else:
                node_dic[x]=node_dic[x]+1
            
        
        
            neighbors=adj_max[x]
       
            parents=[]
        
            for i in neighbors:
                if bfs[i]==level_sorted[a]-1:
                    parents.append(i)
        
        
        
            if len(parents) == 1:
                for j in parents:
                    if j not in node_dic:
                        node_dic[j]=node_dic[x]
                    else:
                        node_dic[j]=node_dic[j]+node_dic[x]
                    
                    
                    
                    if tuple(sorted((x,j))) not in edge_dic:
                        edge_dic[tuple(sorted((x,j)))]=[node_dic[x]]
                    else:
                        edge_dic[tuple(sorted((x,j)))].append(node_dic[x])
                
   
        
            else:
                temp_dic={}
                for j in parents:
                    grand_parents=adj_max[j]
                    count=0
                    for k in grand_parents:
                        if bfs[k]==level_sorted[a]-2:
                            count+=1
                    temp_dic[j]=count
                
                for j in parents:
                    if j not in node_dic:
                        node_dic[j]=node_dic[x]*(temp_dic[j]/sum(temp_dic.values()))
                    else:
                        node_dic[j]=node_dic[j] + (node_dic[x]*(temp_dic[j]/sum(temp_dic.values())))
                   
                    if tuple(sorted((x,j))) not in edge_dic:
                        edge_dic[tuple(sorted((x,j)))]=[node_dic[x]*(temp_dic[j]/sum(temp_dic.values()))]
                    else:
                        edge_dic[tuple(sorted((x,j)))].append(node_dic[x]*(temp_dic[j]/sum(temp_dic.values())))
                    
    return edge_dic
                                   

for x in bfs_nodes:
    betweeness(edge_dic,x)
    
for k,v in edge_dic.items():
    edge_dic[k]=sum(v)/2
 

final=[]
for k,v in edge_dic.items():
    final.append((k,v))

final.sort(key= lambda x:(-x[1], x[0][0]))


with open(bw_op_file,'w') as f:
    for x in final:
        f.write(str(x[0]))
        f.write(",")
        f.write(str(x[1]))
        f.write('\n')


m=len(final)


#candidate_nodes=sc.parallelize(candidate_nodes).map(lambda x:tuple(sorted(x))).distinct().map(lambda x:(x,1)).collectAsMap()

candidate_nodes=candidate_nodes.map(lambda x:tuple(sorted(x))).distinct().map(lambda x:(x,1)).collectAsMap()



mod_dic=[]
kikj={}
nodes_whose_com_hastobecal=[]

for k,v in adj_max.items():
    kikj[k]=len(adj_max[k])

no_of_edges=len(final)


def simple_bfs(graph,vertex):
    explored = []
    queue = [] 
    queue.append(vertex) 
    explored.append(vertex)
    while queue: 
        current = queue.pop(0) 
        for k in graph[current]:
            if k not in explored:
                queue.append(k) 
                explored.append(k)

    return explored

def get_bw_each_step(adj_max_):
    bfs_nodes_=adj_max_.keys()
    edge_dic_={}
    for x in bfs_nodes_:
        betweeness(edge_dic_,x)


    for k,v in edge_dic_.items():
        edge_dic_[k]=sum(v)/2
 

    final_=[]
    for k,v in edge_dic_.items():
        final_.append((k,v))
    
    final_.sort(key= lambda x:(-x[1], x[0][0]))
    return final_ 

def getQ(bwness_list,num_edges,nodes_whose_com_hastobecal):
    
    while num_edges>0:
        
   
        node_removed=bwness_list.pop(0)
        user1=node_removed[0][0]
        user2=node_removed[0][1]


        if user1 in nodes_whose_com_hastobecal:
            nodes_whose_com_hastobecal.remove(user1)
        if user2 in nodes_whose_com_hastobecal:
            nodes_whose_com_hastobecal.remove(user2)

        neighbors_user1=adj_max[user1]
        neighbors_user2=adj_max[user2]
        neighbors_user1.remove(user2)
        neighbors_user2.remove(user1)

        master_list=[]

        bfs_user1=simple_bfs(adj_max,user1)
        master_list.append(bfs_user1)

        bfs_dic1=sc.parallelize(bfs_user1).map(lambda x:(x,1)).collectAsMap()

        if user2 not in bfs_dic1:
            bfs_user2=simple_bfs(adj_max,user2)
            master_list.append(bfs_user2)
            bfs_dic2=sc.parallelize(bfs_user2).map(lambda x:(x,1)).collectAsMap()
        else:
            bfs_dic2={}

        temp_nodes_alloc=[]    
        for noder in nodes_whose_com_hastobecal:
            if noder not in bfs_dic1 and noder not in bfs_dic2:
                temp_nodes_alloc.append(noder)
        
        nodes_whose_com_hastobecal=temp_nodes_alloc
   
        mod_sum=0
        for coms in master_list:
            if len(coms)>1:
                possible_edges=list(combinations(coms,2))
                #print(possible_edges)
                #print('ppppp')
                for edge in possible_edges:
                    edge=tuple(sorted(edge))
                    #print(edge)
                    ki=kikj[edge[0]]
                    kj=kikj[edge[1]]
                    if edge in candidate_nodes:
                        #print()
                        to_add= (1-((ki*kj)/(2*m)))
                        #print(edge,'yes',to_add)
                        mod_sum=mod_sum + to_add
                    else:
                        #print('no')
                        to_add= (0-((ki*kj)/(2*m)))
                        #print(edge,'no',to_add)
                        mod_sum=mod_sum + to_add
                        
           
 
        temp_master_list=[]
        for cal in nodes_whose_com_hastobecal:
            bfs_cal=simple_bfs(adj_max,cal)
            temp_master_list.append(bfs_cal)
            if len(bfs_cal)>1:
                possible_edges_cal=list(combinations(bfs_cal,2))
                for edge_cal in possible_edges_cal:
                    edge_cal=tuple(sorted(edge_cal))
                    ki_cal=kikj[edge_cal[0]]
                    kj_cal=kikj[edge_cal[1]]
                    if edge_cal in candidate_nodes:
                        mod_sum=mod_sum + (1- ((ki_cal*kj_cal)/(2*m)))
                    else:
                        mod_sum=mod_sum + (0- ((ki_cal*kj_cal)/(2*m)))
                        
            

        if len(master_list)>1:
            nodes_whose_com_hastobecal.append(user1)
            nodes_whose_com_hastobecal.append(user2)
    
        for sh in temp_master_list:
            master_list.append(sh)
    
        ######testing########
        check_list=[]
        for g in master_list:
            for r in g:
                check_list.append(r)
        #print(check_list)
        nodes_discon=list(set(bfs_nodes)-set(check_list))
        #print(nodes_discon)
        
        temp_discon_list=[]
        for w in nodes_discon:
            len_chec=len([item for item in temp_discon_list if w in item])
            #print(len_chec)
            if len_chec==0:
                bfs_disc=simple_bfs(adj_max,w)
                #print(bfs_disc)
                temp_discon_list.append(bfs_disc)
                if len(bfs_disc)>1:
                    possible_edges_disc=list(combinations(bfs_disc,2))
                    for edge_disc in possible_edges_disc:
                        edge_disc=tuple(sorted(edge_disc))
                        ki_disc=kikj[edge_disc[0]]
                        kj_disc=kikj[edge_disc[1]]
                        if edge_disc in candidate_nodes:
                            mod_sum=mod_sum + (1- ((ki_disc*kj_disc)/(2*m)))
                        else:
                            mod_sum=mod_sum + (0- ((ki_disc*kj_disc)/(2*m)))
            
        
        for she in temp_discon_list:
            master_list.append(she)
        #print(temp_discon_list)    
        ###################
        
        
        mod_sum=mod_sum/(2*m)
        #print(mod_sum)

        mod_dic.append((master_list,mod_sum))
        next_betweeness=get_bw_each_step(adj_max)
        bwness_list=next_betweeness
        num_edges=num_edges-1
    
    return mod_dic
 
ans=getQ(final,no_of_edges,nodes_whose_com_hastobecal)  
df=sc.parallelize(ans).sortBy(lambda x:-x[1])
gh=df.take(1)
gh=gh[0][0]
res=sc.parallelize(gh).map(lambda x:(x,len(x))).map(lambda x:(sorted(x[0]),x[1])).sortBy(lambda x:(x[1],x[0][0])).map(lambda x:x[0]).collect()

with open(com_op_file,'w') as f:
    for x in res:
        f.write(str(x)[1:-1])
        f.write("\n")



end=time.time()
print("Duration:",end-start)