# DataMiningINF553
Projects and assignments done as part of INF 553 at USC

All the work has been carried out extensively by using Python and Spark.
Additionally, some streaming APIs such as Tweepy have been used at times.


HW1 - 

Task1: Basic data exploration using Spark

Task2: The two datasets together (i.e., review and business) are explored together and we
compute the average stars for each business category and output top n categories with the highest
average stars(1pts).

Task3: We compute the businesses that have more than n reviews in the review file (1pts). At the same time, we show the number of partitions
for the RDD and the number of items per partition with either default or customized partition function


HW2 -

We implement the SON algorithm using the Apache Spark Framework.
We also develop a program to find frequent itemsets in two datasets, one simulated dataset
and one real-world dataset generated from Yelp dataset.


HW3 - 

We leverage principles of Min-Hash, Locality
Sensitive Hashing (LSH), to develop recommendation system - CONTENT BASED and COLLABORATIVE FILTERING (Item based and User based CF)


HW4 - 

We explore the spark GraphFrames library as well as implement our
own Girvan-Newman algorithm using the Spark Framework to detect communities in graphs,ie, users who have a similar business taste.


HW5 - 

We implement clustering algorithms such as K-Means and Bradley-Fayyad-Reina (BFR) algorithm. 


HW6 -

We implement three algorithms: the Bloom filtering, Flajolet- Martin algorithm, and reservoir sampling. For the first task,we implement Bloom
Filtering for off-line Yelp business dataset. The “off-line” here means tbat we do not need to take
the input as streaming data. For the second and the third task, we deal with on-line
streaming data directly. In the second task,we generate a simulated data stream
with the Yelp dataset and implement Flajolet-Martin algorithm with Spark Streaming library.
In the third task, we do some analysis on Twitter stream using fixed size sampling (Reservoir Sampling).


Hybrid Recommendation system - 

1) Leveraged 1 million rows of business-user related Yelp data to build a recommendation system using Spark
2) Item based collaborative filtering and XgBoost helped the system perform better than 90% of other models
