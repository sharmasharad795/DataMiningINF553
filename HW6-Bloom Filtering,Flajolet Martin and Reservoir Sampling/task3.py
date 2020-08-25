import tweepy
import random
import sys

port=int(sys.argv[1])
op_file=sys.argv[2]

count_tw=0
list_tweets=[]
random_nos=[]

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, tw):
        global count_tw
        global list_tweets
        global random_nos
        hasht=tw.entities['hashtags']
        #print(hasht)
        for x in hasht:
            if x['text']!='':
                count_tw+=1
                random_nos.append(count_tw)
                if count_tw <= 100:
                    list_tweets.append(hasht)
                else:
                    choice=random.choice(random_nos)
                    if choice <= 100:
                        tobe_replaced=random.choice(list_tweets)
                        index_tobe_replaced=list_tweets.index(tobe_replaced)
                        list_tweets[index_tobe_replaced]=hasht
                    #print(len(list_tweets))    
                hash_dic={}
                for x in list_tweets:
                    for y in x:
                        tag=y['text']
                        if tag not in hash_dic:
                            hash_dic[tag]=1
                        else:
                            hash_dic[tag]=hash_dic[tag]+1
                hash_dic=[(k, v) for k, v in hash_dic.items()] 
                temp1 = set(list({x[1] for x in hash_dic})[-3:])
                temp1 = list(filter(lambda x: x[1] in temp1, hash_dic))
                hash_dic=sorted(temp1, key=lambda x:(-x[1],x[0]))
                    
                with open(op_file,"a+") as f:
                    f.write("The number of tweets with tags from the beginning: " + str(count_tw))
                    f.write('\n')
                    for x in hash_dic:
                        f.write(str(x[0])+ " : "+str(x[1]))
                        f.write('\n')
                    f.write('\n')
                    
    def on_error(self, status_code):
        if status_code == 420:
            return False


auth = tweepy.OAuthHandler('9iBGenwZsZSLTErimZ4UYfK7Y', 'fz2bTPaDs6z48Xf1yS4QKr96iB7bcc8a8xw19mJYJNrGmjSh7G')
auth.set_access_token('523021974-iMzHZ0jUOn0lYmdjPpct49DKYxiQC08M804MHePu', 'MQpaQZkgKPWXHZqr5CFPV5mN4F3vZPP7bZ4LL8X3OVRAY')
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

myStream.filter(track=["Vaccine", "Modi", "Trump", "Netflix", "Football"])


