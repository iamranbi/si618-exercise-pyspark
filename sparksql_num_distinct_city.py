import json
import os
from pyspark import SparkContext
sc = SparkContext(appName="yelp")
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

review = sqlContext.read.json("hdfs:///var/si618f17/yelp_academic_dataset_review.json")
business = sqlContext.read.json("hdfs:///var/si618f17/yelp_academic_dataset_business.json")

#review.printSchema()
#business.printSchema()
review.registerTempTable('reviewt')
business.registerTempTable('businesst')
r1 = sqlContext.sql('select business_id as b_id, user_id, stars from reviewt')
r1.registerTempTable('r2')
b1 = sqlContext.sql('select business_id, city from businesst')
b1.registerTempTable('b2')
#join
rb1 = sqlContext.sql('select * from r2 join b2 on b_id = business_id')
rb1.registerTempTable('rb11')
rb2all = sqlContext.sql('select user_id, city from rb11 order by user_id')
rb2pos = sqlContext.sql('select user_id, city from rb11 where stars>3 order by user_id')
rb2neg = sqlContext.sql('select user_id, city from rb11 where stars<3 order by user_id')

rb3all=rb2all.dropDuplicates()
rb3all.registerTempTable('rb31all')
rb3pos=rb2pos.dropDuplicates()
rb3pos.registerTempTable('rb31pos')
rb3neg=rb2neg.dropDuplicates()
rb3neg.registerTempTable('rb31neg')

rb4all = sqlContext.sql('select user_id, count(*) as cities from rb31all group by user_id order by cities desc')
rb4pos = sqlContext.sql('select user_id, count(*) as cities from rb31pos group by user_id order by cities desc')
rb4neg = sqlContext.sql('select user_id, count(*) as cities from rb31neg group by user_id order by cities desc')

cities_histogram_all = rb4all.select('cities').rdd.flatMap(lambda x: x).histogram(30)
cities_histogram_pos = rb4pos.select('cities').rdd.flatMap(lambda x: x).histogram(24)
cities_histogram_neg = rb4neg.select('cities').rdd.flatMap(lambda x: x).histogram(15)
numall = list(range(1, 31))
numpos = list(range(1, 25))
numneg = list(range(1, 16))
citiesall=zip(list(numall),list(cities_histogram_all)[1])
citiespos=zip(list(numpos),list(cities_histogram_pos)[1])
citiesneg=zip(list(numneg),list(cities_histogram_neg)[1])

allr=sc.parallelize(citiesall).toDF(['cities', 'yelp users'])
posr=sc.parallelize(citiespos).toDF(['cities', 'yelp users'])
negr=sc.parallelize(citiesneg).toDF(['cities', 'yelp users'])

header=sc.parallelize(["cities, yelp users"])
a=allr.rdd.map(lambda x: unicode(x[0])+','+ unicode(x[1]))
p=posr.rdd.map(lambda x: unicode(x[0])+','+ unicode(x[1]))
n=negr.rdd.map(lambda x: unicode(x[0])+','+ unicode(x[1]))

header.union(a).saveAsTextFile(("hdfs:///user/rbi/output_allreview"))
header.union(p).saveAsTextFile(("hdfs:///user/rbi/output_goodreview"))
header.union(n).saveAsTextFile(("hdfs:///user/rbi/output_badreview"))
