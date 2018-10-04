import json
from pyspark import SparkConf, SparkContext

sc = SparkContext(appName="StarsPerCategory")

input_file = sc.textFile("hdfs:///var/si618f17/yelp_academic_dataset_business.json")

def cat_da(data):
    cat_da = []
    stars = data.get('stars', None)
    city = data.get('city', None)
    neighborhoods = data.get('neighborhoods', None)
    if len(neighborhoods) < 1:
        neighborhoods = ['Unknown']
    review_count = data.get('review_count', None)
    if neighborhoods :
        for n in neighborhoods:
            if stars >= 4:
                stars = 1
            else:
                stars = 0
            cat_da.append((city, n, review_count, stars))
    return cat_da

cat_da1 = input_file.map(lambda line: json.loads(line)) \
                      .flatMap(cat_da) \
                      .map(lambda x: ((x[0], x[1]), (x[2], x[3]))) \
                      .mapValues(lambda x: (1,x[0], x[1])) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
#city; neighborhoods; count; totoal review; 4star review
cat_da2 = cat_da1.map(lambda x : (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2]))
cat_da_sorted = cat_da2.sortBy(lambda x: (x[0],-x[2],-x[3],-x[4],x[1]))
#cat_da_sorted1 = cat_da_sorted2.sortBy(lambda x: (x[2],x[3],x[4]), False)
#cat_da_sorted = cat_da_sorted1.sortBy(lambda x: x[0], ascending = True)

cat_da_sorted.map(lambda x: unicode(x[0])+'\t'+unicode(x[1])+'\t'+str(x[2])+'\t'+str(x[3])+'\t'+str(x[4])).saveAsTextFile(("hdfs:///user/rbi/si618_f17_hw3_output_rbi"))
