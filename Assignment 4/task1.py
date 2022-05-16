from pyspark     import SparkContext, SparkConf
from pyspark.sql import SparkSession as spark, SQLContext, Row
import sys
from time import time
from graphframes import GraphFrame

def generate_edge_rdd(rdd_row): 
    edge_list = []
    business_set = user_bid_map[rdd_row[0]]
    i = user_bid_collected_data.index((rdd_row[0], business_set))
    for j in range(i+1, len(user_bid_map)):
        if len(business_set.intersection(user_bid_collected_data[j][1])) >= filter_threshold:
            edge_list.append((rdd_row[0], user_bid_collected_data[j][0]))
            edge_list.append((user_bid_collected_data[j][0], rdd_row[0]))
    return edge_list

def generate_vertex_rdd(rdd_row):
    vertex_set = set()
    for v in rdd_row:
        vertex_set.add(v)
    return vertex_set

def build_vertex_data(vertex_set):
    vertex_data = []
    for v in vertex_set:
        vertex_data.append((v,))
    return vertex_data


start_time = time()
conf = SparkConf().setAppName('DM_assign4_task1').setMaster('local[*]')
sc   = SparkContext.getOrCreate(conf = conf)
sparky = spark(sc)
sqlContext = SQLContext(sc)

sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
sc.setLogLevel('ERROR')

#filter_threshold, input_file, community_output_file = 7, 'ub_sample_data.csv', 'abcd.txt'
filter_threshold, input_file, community_output_file = int(sys.argv[1]), sys.argv[2], sys.argv[3]
input_data_rdd          = sc.textFile(input_file)\
                            .map(lambda row : row.split(','))
        
csv_header_data      = input_data_rdd.first()
input_data_rdd       = input_data_rdd.filter(lambda row : row != csv_header_data)\
                                        .map(lambda row : [row[0], set([row[1]])])\
                                        .reduceByKey(lambda row1, row2 : row1 | row2)
        
user_bid_collected_data = input_data_rdd.collect()
user_bid_map            = input_data_rdd.collectAsMap()

edge_data_rdd           = input_data_rdd.map(lambda row : generate_edge_rdd(row))\
                                        .filter(lambda row: row != [])\
                                        .flatMap(lambda row: row)

# Full Final
edge_data               = edge_data_rdd.collect()
#print(edge_data)
edge_DF = sqlContext.createDataFrame(edge_data, ["src", "dst"])

#print(edge_data_rdd.take(20))
user_vertex_data        = edge_data_rdd.map(lambda row : generate_vertex_rdd(row))\
                                       .collect()


vertex_data = build_vertex_data(set().union(*user_vertex_data))

vertex_DF = sqlContext.createDataFrame(vertex_data, ['id'])

g = GraphFrame(vertex_DF ,edge_DF)

communities = g.labelPropagation(maxIter=5)

community_rdd = communities.rdd

#community_rdd.first()

community_rdd = community_rdd.map(lambda x: (str(x[1]), set([x[0]]))).reduceByKey(lambda x,y : x | y).mapValues(list)

community = community_rdd.collectAsMap()

for k, v in community.items():
    v.sort()

community_items = sorted(community.items(), key = lambda x : (len(x[1]), x[1]))

last_idx = len(community_items) - 1
print(community_output_file)
print(community_items)
with open(community_output_file, 'w') as f:
    for idx, community_item in enumerate(community_items):
        _, user_id_list = community_item
        s = ''
        for user in user_id_list:
            s += "'" + user + "'" + ',' + ' '
            # s += "'"
            # s += user
            # s += "'"
            # s += ','
            # s += ' '
        s = s.rstrip(', ')
        f.write(s)
        if idx != last_idx:
            f.write('\n')
