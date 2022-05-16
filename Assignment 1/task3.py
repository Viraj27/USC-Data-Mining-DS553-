from pyspark import SparkContext, SparkConf
import datetime
import json
import time
import sys
from operator import add

def evaluate_average_stars_per_city(test_review_data, business_data):
    
    # for each review, get the business ID and average stars. 
    #Then, search that business ID in business_data and find corresponding city. Create a dict for city -> stars.

    business_stars_rdd = test_review_data.map(lambda x : (x["business_id"], (x["stars"],1)))\
                                         .reduceByKey(lambda x, y : (x[0] + y[0], x[1] + y[1]))
    
    """aggregated_business_stars_rdd = business_stars_rdd.aggregateByKey(a_tuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                               lambda a,b: (a[0] + b[0], a[1] + b[1]))

    average_business_stars_rdd = aggregated_business_stars_rdd.mapValues(lambda v: v[0]/v[1])

    # In business_data, go through each entry, extract business id, find avg stars on dict and avg that again on city.
    """


    business_city_rdd = business_data.map(lambda x : (x["business_id"], x.get("city","")))
    joined_rdd = business_stars_rdd.join(business_city_rdd)
    joined_rdd.map(json.loads)
    final_rdd = joined_rdd.map(lambda x : (x[1][1], x[1][0]))\
                               .reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))\
                               .map(lambda x : (x[0], x[1][0] / x[1][1]))
                               
    #aggregated_city_stars_rdd = city_stars_map.aggregateByKey(a_tuple, lambda a,b: (a[0] + b,    a[1] + 1),
    #                                           lambda a,b: (a[0] + b[0], a[1] + b[1]))
    
    #final_rdd = city_stars_map.mapValues(lambda v : v[0]/v[1])
    
    return final_rdd

conf = SparkConf().setAppName('DM_assign1').setMaster('local[*]')
sc   = SparkContext.getOrCreate(conf = conf)

sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
sc.setLogLevel('ERROR')

# input args capture

review_file_path, business_file_path, output_file_path_a, output_file_path_b = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# Task 3 A
test_review_data = sc.textFile(review_file_path).map(json.loads)
business_data    = sc.textFile(business_file_path).map(json.loads)
#test_review_data = sc.textFile('test_review.json').map(json.loads)
#business_data    = sc.textFile('business.json').map(json.loads)

final_rdd = evaluate_average_stars_per_city(test_review_data, business_data).sortBy(lambda x : (-x[1], x[0]))

with open(output_file_path_a, 'w') as of:
    of.write('city,stars')
    of.write('\n')
    for f in final_rdd.collect():
        of.write(str(f[0]))
        of.write(',')
        of.write(str(f[1]))
        of.write('\n')


#Q3 B start

# The dictionary which will contain answers for all Task 3 questions to be dumped to JSON file.
output_json_dict = {}

#****** Python sort and print ******

st_time = time.time()
test_review_data = sc.textFile(review_file_path).map(json.loads)
business_data    = sc.textFile(business_file_path).map(json.loads)
#test_review_data = sc.textFile('test_review.json').map(json.loads)
#business_data    = sc.textFile('business.json').map(json.loads)

final_rdd = evaluate_average_stars_per_city(test_review_data, business_data)

finalResult_list = sorted(final_rdd.collect(), key = lambda x : (-x[1], x[0]))[:10]

print(finalResult_list)

end_time = time.time()

output_json_dict["m1"] = end_time - st_time

#*********** Spark sort and print **************
st_time = time.time()
test_review_data = sc.textFile(review_file_path).map(json.loads)
business_data    = sc.textFile(business_file_path).map(json.loads)\
#test_review_data = sc.textFile('test_review.json').map(json.loads)
#business_data    = sc.textFile('business.json').map(json.loads)

final_rdd = evaluate_average_stars_per_city(test_review_data, business_data)

final_rdd = final_rdd.sortBy(lambda x : (-x[1], x[0]))

print(final_rdd.take(10))
end_time = time.time()
output_json_dict["m2"] = end_time - st_time

# reason
reason = "The Spark SortBy goes through the following transformations: 1. Input RDD is sampled and this sample is used to compute boundaries for each output partition (sample followed by collect).2. Input RDD is partitioned using rangePartitioner with boundaries computed in the first step (partitionBy).\
3. Each partition from the second step is sorted locally (mapPartitions). 4. Spark shuffles the data into sorted partitions.\
5. The data is then collected all together to return the sorted RDD. This approach is very expensive because of the multi-phase approach to sorting coupled with the additional shuffle operation which is very expensive.\n\
For python sorting, the RDD data is collected into a list and then sorted efficiently via Python's TimSort approach which requires none of the expensive RDD operations \n\
and thus, is executed in lesser time."

output_json_dict["reason"] = reason

with open(output_file_path_b, 'w') as f:
    json.dump(output_json_dict, f, indent = 4)
