from collections import defaultdict
from itertools import combinations
from pyspark     import SparkContext, SparkConf
import sys
import time


def get_item_count_per_candidate(baskets,candidate_sets):
    counts_dict = defaultdict(int)
    baskets      = list(baskets)
    for i in candidate_sets:
        for b in baskets:
            if i.issubset(b):
                counts_dict[i] += 1
    return counts_dict.items()

def find_candidate_itemset(frequent_items,combination):
    frequent_items_length = len(frequent_items)
    candidate_items_set = set()
    for i in range(frequent_items_length-1):
        for j in range(i+1,frequent_items_length):
            candidate_set = frequent_items[i].union(frequent_items[j])
            if len(candidate_set) == combination:
                candidate_items_set.add(candidate_set)
    return candidate_items_set

def populate_frequent_items_list(counts_dict, frequent_items, all_frequent_items_list, ps):
    for k,v in counts_dict.items(): 
        if v < ps:
            continue
        frequent_items.append(k)
        all_frequent_items_list.append((k,1))
    return frequent_items, all_frequent_items_list

def find_frequent_itemset(candidate_items_set, basket_list,ps, all_frequent_items_list):
    counts_dict = defaultdict(int)
    for i in candidate_items_set:
        for j in basket_list:
            if i.issubset(j):
                counts_dict[i] += 1
    
    frequent_items = []
    frequent_items, all_frequent_items_list = populate_frequent_items_list(counts_dict, frequent_items, all_frequent_items_list, ps)
    return frequent_items,all_frequent_items_list

def find_frequent_itemsets(baskets, support, items_total):
    baskets_list            = [] 
    all_frequent_items_list = []
    frequent_items          = []
    counts_dict             = defaultdict(int)
    combinations            = 2

    for b in baskets:
        baskets_list.append(b)
        for i in b:
            counts_dict[ frozenset( (i,) ) ] += 1

    ps = support * (len(baskets_list) / items_total)
    
    frequent_items, all_frequent_items_list = populate_frequent_items_list(counts_dict, frequent_items, all_frequent_items_list, ps)

    while True:
        candidate_items_list                         = find_candidate_itemset(frequent_items,combinations)
        frequent_items, all_frequent_items_list      = find_frequent_itemset(candidate_items_list, baskets_list,ps, all_frequent_items_list)

        if len(candidate_items_list) == 0 or len(frequent_items) == 0:
            break
        combinations += 1
    return all_frequent_items_list

def format_output(items_set,f):
    itemset_length_dict = defaultdict(lambda : [])
    for i in items_set:
        itemset_length_dict[len(i)].append(sorted(tuple(i)))
    for i in itemset_length_dict.values():
        i.sort()
        f.write(','.join(list(map(lambda val: str(tuple(val)).replace(',)',')'),i))))
        f.write('\n\n')

def write_output(candidate_sets,frequent_sets, output_file):
    with open(output_file,'w+') as f:
        f.write('Candidates:\n')
        format_output(candidate_sets,f)
        f.write('Frequent Itemsets:\n')
        format_output(frequent_sets,f)

def task_1(input_data_rdd,support,case, output_file):
    """Use the SON Limited-Pass algorithm to find the frequent itemsets in the input data"""
    header_data    = input_data_rdd.first()
    input_data_rdd = input_data_rdd.filter(lambda row: row != header_data)
    
    #Basket creation
    # Combinations of frequent businesses.
    if case == 1:
        baskets = input_data_rdd.map(lambda row : (row[0],frozenset( [row[1]] ) ))
                  
    # Combinations of frequent users.
    else:
        baskets = input_data_rdd.map(lambda row : (row[1],frozenset([row[0]])))

    baskets     = baskets.reduceByKey(lambda x,y : x | y)\
                         .map(lambda x: x[1])

    item_total     = baskets.count()
    candidate_sets = baskets.mapPartitions(lambda row: find_frequent_itemsets(row,support,item_total))\
                            .groupByKey()\
                            .map(lambda x: x[0])\
                            .collect()

    frequent_sets = baskets.mapPartitions(lambda row : get_item_count_per_candidate(row,candidate_sets))\
                           .reduceByKey(lambda x,y: x + y)\
                           .filter(lambda row: row[1] >= support)\
                           .map(lambda row :list(row[0]))\
                           .collect()
    write_output(candidate_sets, frequent_sets, output_file)


conf = SparkConf().setAppName('DM_assign2_task1').setMaster('local[*]')
sc   = SparkContext.getOrCreate(conf = conf)

sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
sc.setLogLevel('ERROR')

case ,support,input_file,output_file = int(sys.argv[1]),int(sys.argv[2]),str(sys.argv[3]),str(sys.argv[4])
st_time        = time.time()
input_data_rdd = sc.textFile(input_file)\
                    .map(lambda row : row.split(','))
task_1(input_data_rdd, support, case, output_file)
print('Duration: {0}'.format(time.time() - st_time))