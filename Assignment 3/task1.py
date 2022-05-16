from pyspark     import SparkContext, SparkConf
from itertools   import combinations
import random
import sys
import csv

_HASH_FUNC_NUM = 100
_PRIME_NUMBER  = 999999991
_BANDS         = 50
_ROWS          = 2
_SIMILAIRTY_VALUE_THRESHOLD = 0.5

class Task1():
    def setup(self):
        conf = SparkConf().setAppName('DM_assign3_task1').setMaster('local[*]')
        sc   = SparkContext.getOrCreate(conf = conf)

        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        sc.setLogLevel('ERROR')

        return sc

    def read_and_pre_process_input_file(self, sc):

        input_file, output_file = sys.argv[1], sys.argv[2]
        input_data_rdd          = sc.textFile(input_file)\
                                .map(lambda row : row.split(','))
        
        csv_header_data = input_data_rdd.first()
        input_data_rdd  = input_data_rdd.filter(lambda row : row != csv_header_data)
        
        return input_data_rdd, output_file

    def get_user_idx_info(self, input_data_rdd):
        users_idx_map = {}
        users = input_data_rdd.map(lambda row: row[0]).distinct().collect()
        for idx, user in enumerate(users):
            users_idx_map[user] = idx
        users_cnt = len(users_idx_map)

        return users_idx_map, users_cnt

    def get_biz_idx_info(self, input_data_rdd):
        biz_idx_map, idx_biz_map = {}, {}
        biz  = input_data_rdd.map(lambda row: row[1]).distinct().collect()
        for idx, business in enumerate(biz):
            biz_idx_map[business] = idx
        
        for key in biz_idx_map:
            idx_biz_map[biz_idx_map[key]] = key
        
        return biz_idx_map, idx_biz_map

    def get_hash_functions(self, user_cnt):

        def generate_hash_function(user_cnt):
            # form - f(x) = ((ax+b)%m)%user_cnt

            a, b = random.randint(2, 10000000000), random.randint(2, 10000000000)

            return lambda x : ((a*x+b)%_PRIME_NUMBER)%user_cnt
            

        hash_functions_list = []
        for _ in range(_HASH_FUNC_NUM):
            hash_func = generate_hash_function(user_cnt)
            hash_functions_list.append(hash_func) 
        return hash_functions_list

    def build_signature_matrix(self, row, hash_func_list):
        hash_row = [float('inf') for _ in range(_HASH_FUNC_NUM)]
        for uidx in row[1]:
            for h in range(_HASH_FUNC_NUM):
                func_hash           = hash_func_list[h]
                #val_hash            = func_hash(uidx)
                hash_row[h] = min(func_hash(uidx), hash_row[h])
        return hash_row


    def create_signature_matrix(self, input_data_rdd, user_idx_map, biz_idx_map, hash_func_list):
        

        biz_user_map = input_data_rdd.map(lambda row: (biz_idx_map[row[1]], {user_idx_map[row[0]]}))\
                                     .reduceByKey(lambda row1, row2 : row1 | row2)

        return biz_user_map, biz_user_map.map(lambda row : (row[0], self.build_signature_matrix(row, hash_func_list)))

    def get_similar_pairs(self, signature_matrix_rdd, biz_user_map):

        def locality_sensitive_hashing(table_r):
            bucket_list = list()
            for i in range(_BANDS):
                bucket_list.append(((tuple(table_r[1][i*_ROWS:i*_ROWS + _ROWS]), i), table_r[0]))
            return bucket_list

        def find_similar_pairs(table_r, biz_user_map):

            def find_jaccard_similarity(pair, biz_user_map):
                return len(biz_user_map.get(pair[0]).intersection(biz_user_map.get(pair[1]))) / len(biz_user_map.get(pair[0]).union(biz_user_map.get(pair[1])))
            
            similar_pairs_dict = {}
            candidate_pairs    = set(combinations(table_r[1], 2))
            for candidate in candidate_pairs:
                similarity_value = find_jaccard_similarity(candidate, biz_user_map)
                if similarity_value >= _SIMILAIRTY_VALUE_THRESHOLD:
                    similar_pairs_dict[candidate] = find_jaccard_similarity(candidate, biz_user_map)
            return similar_pairs_dict.items()

        biz_user_data = biz_user_map.collectAsMap()
        
        return signature_matrix_rdd.flatMap(lambda row : locality_sensitive_hashing(row))\
                                   .groupByKey()\
                                   .map(lambda row: (row[0], sorted(set(row[1]))))\
                                   .flatMap(lambda row: find_similar_pairs(row, biz_user_data))\
                                   .collectAsMap()

    def write_results_to_csv(self, similar_biz_dict, idx_biz_map, output_file):
        
        similar_biz_id_pairs = list()
        for biz_pairs in similar_biz_dict:
                biz_id_pairs = sorted((idx_biz_map[biz_pairs[0]], idx_biz_map[biz_pairs[1]]))
                similar_biz_id_pairs.append((biz_id_pairs, similar_biz_dict[biz_pairs]))
        
        similar_biz_id_pairs = sorted(similar_biz_id_pairs)
        with open(output_file,'w+') as f:
            writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
            writer.writerow(["business_id_1", " business_id_2", " similarity"])
            for biz_pairs, rating in similar_biz_id_pairs:
                writer.writerow([biz_pairs[0], biz_pairs[1], rating])
         



    def task1(self):
        sc                                  = self.setup()
        input_data_rdd, output_file         = self.read_and_pre_process_input_file(sc)
        user_idx_map, user_cnt              = self.get_user_idx_info(input_data_rdd)
        biz_idx_map, idx_biz_map            = self.get_biz_idx_info(input_data_rdd)
        hash_functions_list                 = self.get_hash_functions(user_cnt)
        biz_user_map, signature_matrix_rdd  = self.create_signature_matrix(input_data_rdd, user_idx_map, biz_idx_map, hash_functions_list) 
        similar_biz_map                     = self.get_similar_pairs(signature_matrix_rdd, biz_user_map)
        self.write_results_to_csv(similar_biz_map, idx_biz_map, output_file)


def main():
    t1 = Task1()
    t1.task1()
        

if __name__ == '__main__':
    main()