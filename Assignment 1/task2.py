from pyspark import SparkContext, SparkConf
import json
import time
import sys
from collections import defaultdict
review_file_path, output_file_path, n_partitions = sys.argv[1], sys.argv[2], int(sys.argv[3])
conf            = SparkConf().setAppName('DM_assign1').setMaster('local[*]')
sc              = SparkContext.getOrCreate(conf = conf)
sc.setLogLevel('ERROR')
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
st_time         = time.time()
result_dict     = defaultdict(dict)

#****for reference
"""# Task 1 Q6
distinct_businesses_reviewed = test_review.map(lambda x : (x["business_id"], 1)).reduceByKey(lambda a,b : a + b).sortBy(lambda x : (-x[1], x[0]))
output_json_dict["top10_business"] = distinct_businesses_reviewed.take(10)"""

# Default implementation
review_rdd       = sc.textFile(review_file_path).map(json.loads)
st_time          = time.time()
default_impl_rdd = review_rdd.map(lambda x : (x["business_id"], 1)).reduceByKey(lambda a,b : a + b).sortBy(lambda x : (-x[1], x[0]))
end_time         = time.time()

result_dict["default"]["n_partition"] = default_impl_rdd.getNumPartitions()
result_dict["default"]["n_items"]     = default_impl_rdd.glom().map(len).collect()
result_dict["default"]["exe_time"]    = end_time - st_time 

# Customized implementation
st_time             = time.time()
#review_rdd          = sc.textFile(review_file_path).map(json.loads)
# modulo a huge prime number
customized_impl_rdd = review_rdd.map(lambda x: (x["business_id"], 1)).partitionBy(n_partitions, lambda x: ord(x[0]))
customized_impl_rdd = customized_impl_rdd.reduceByKey(lambda a,b : a + b)
customized_output   = customized_impl_rdd.takeOrdered(10, lambda x : (-x[1], x[0]))
end_time            = time.time()

result_dict["customized"]["n_partition"] = customized_impl_rdd.getNumPartitions()
result_dict["customized"]["n_items"]     = customized_impl_rdd.glom().map(len).collect()
result_dict["customized"]["exe_time"]    = end_time - st_time 

with open(output_file_path, 'w') as f:
    json.dump(result_dict, f, indent = 4)
#print(result_dict)