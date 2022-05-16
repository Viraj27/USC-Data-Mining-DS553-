# Task 1 final .py cell

from pyspark import SparkContext, SparkConf
import datetime
import json
import sys

conf = SparkConf().setAppName('DM_assign1').setMaster('local[*]')
sc   = SparkContext.getOrCreate(conf = conf)
sc.setLogLevel('ERROR')

sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')

# The dictionary which will contain answers for all Task 1 questions to be dumped to JSON file.
output_json_dict = {}

#helper functions

def reviews_2018(x):
    date_str = x['date']
    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    return date_obj.year == 2018

# input args capture

review_file_path, output_file_path = sys.argv[1], sys.argv[2]

test_review = sc.textFile(review_file_path).map(json.loads)

# Task 1 Q1
keys = test_review.map(lambda x : x["review_id"])
output_json_dict["n_review"] = keys.count()

# Task 1 Q2
reviews_from_2018 = test_review.filter(lambda x : reviews_2018(x))
output_json_dict["n_review_2018"] = reviews_from_2018.count()

# Task 1 Q3
unique_users = test_review.map(lambda x : (x["user_id"], 1)).reduceByKey(lambda a,b : a + b)
output_json_dict["n_user"] = unique_users.count()

# Task 1 Q4
unique_largest_users = test_review.map(lambda x : (x["user_id"], 1)).reduceByKey(lambda a,b : a + b).sortBy(lambda x : (-x[1], x[0]))
output_json_dict["top10_user"] = unique_largest_users.take(10)

# Task 1 Q5
distinct_businesses_reviewed = test_review.map(lambda x : (x["business_id"], 1)).reduceByKey(lambda a,b : a + b)
output_json_dict["n_business"] = distinct_businesses_reviewed.count()

# Task 1 Q6
distinct_businesses_reviewed = test_review.map(lambda x : (x["business_id"], 1)).reduceByKey(lambda a,b : a + b).sortBy(lambda x : (-x[1], x[0]))
output_json_dict["top10_business"] = distinct_businesses_reviewed.take(10)

with open(output_file_path, 'w+') as f:
    json.dump(output_json_dict, f, indent = 4, ensure_ascii = False)