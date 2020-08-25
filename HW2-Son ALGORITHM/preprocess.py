import json
import csv


review = sc.textFile("/Users/sharadsharma/Downloads/review.json").map(lambda x: (json.loads(x)['business_id'],json.loads(x)['user_id'])).persist()
business = sc.textFile("/Users/sharadsharma/Downloads/business.json").map(lambda x: (json.loads(x)['business_id'],json.loads(x)['state'])).filter(lambda x:x[1]=='NV').persist()
join = review.join(business).map(lambda x:(x[1][0],x[0])).collect()

with open("/work/user_business.csv",'w') as f:
    file=csv.writer(f)
    file.writerow(['user_id','business_id'])
    file.writerows(join)




