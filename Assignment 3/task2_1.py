from pyspark     import SparkContext, SparkConf
from time        import time
import sys
import math
import csv

class Task2Point1():

    def __init__(self):
        self.pearson_similarities = {}

    def setup(self):
        conf = SparkConf().setAppName('DM_assign3_task2point1').setMaster('local[*]')
        sc   = SparkContext.getOrCreate(conf = conf)

        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        sc.setLogLevel('ERROR')

        return sc

    def create_rdd_from_file(self, sc, file):

        rdd                = sc.textFile(file).map(lambda row : row.split(','))
        csv_header_data    = rdd.first()
        rdd                = rdd.filter(lambda row : row != csv_header_data)

        return rdd

    def read_and_pre_process_input_file(self, sc):

        training_file, test_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]

        training_data_rdd = self.create_rdd_from_file(sc, training_file).map(lambda row : (row[0], row[1], float(row[2])))
        test_data_rdd     = self.create_rdd_from_file(sc, test_file).map(lambda row : (row[0], row[1]))
        return training_data_rdd, test_data_rdd, output_file

    def generate_training_data_maps(self, training_data_rdd):

        self.user_biz_rating_map = training_data_rdd.map(lambda row : (row[0], [(row[1],row[2])]))\
                                               .reduceByKey(lambda row1, row2 : row1 + row2)\
                                               .collectAsMap()

        self.biz_user_rating_map = training_data_rdd.map(lambda row : (row[1], [(row[0],row[2])]))\
                                               .reduceByKey(lambda row1, row2 : row1 + row2)\
                                               .collectAsMap()

    
    # def find_all_rated_businesses(self, uid, user_biz_rating_map):

    #     biz_rating_list = user_biz_rating_map.get(uid)

    #     return [biz_rating[0] for biz_rating in biz_rating_list]

    def find_all_users_rating_this_business(self, bid, biz_user_rating_map):

        user_rating_list = biz_user_rating_map.get(bid)

        return set(user_rating[0] for user_rating in user_rating_list)

    def evaluate_pearson_similarity(self, bid, business):

        # Similarity already calculated for the business
        key_tuple = tuple(sorted((bid, business)))
        if key_tuple in self.pearson_similarities:
            return self.pearson_similarities[key_tuple]
            
        user_set_for_bid      = self.find_all_users_rating_this_business(bid, self.biz_user_rating_map)
        user_set_for_business = self.find_all_users_rating_this_business(business, self.biz_user_rating_map)

        # find the U for evaluating pearson similarity
        common_user_set       = user_set_for_bid.intersection(user_set_for_business)

        # No users that have rated both items - assign similarity based on how close average ratings are for both items.
        if len(common_user_set) == 0:
            diff_avg_rating = abs(self.business_average_rating[bid] - self.business_average_rating[business])
            if diff_avg_rating <= 1:
                similarity = 0.85
            elif diff_avg_rating <=2:
                similarity = 0.35
            else:
                similarity = 0
            self.pearson_similarities[key_tuple] = similarity
            return similarity

        num = 0.0
        root1, root2 = 0.0,0.0
        user_ratings_map_bid      = dict(self.biz_user_rating_map.get(bid))
        user_ratings_map_business = dict(self.biz_user_rating_map.get(business))
        for user in common_user_set:
            #business_ratings_map = dict(self.user_biz_rating_map.get(user))
            num += (user_ratings_map_bid.get(user) - self.business_average_rating.get(bid)) * (user_ratings_map_business.get(user) - self.business_average_rating.get(business))
            root1 += (user_ratings_map_bid.get(user) - self.business_average_rating.get(bid)) ** 2
            root2 += (user_ratings_map_business.get(user) - self.business_average_rating.get(business)) ** 2
        
        denom      = math.sqrt(root1) * math.sqrt(root2)

        if denom == 0:
            similarity = 0
        else:
            similarity = num / denom

        self.pearson_similarities[key_tuple] = similarity

        return similarity

    def predict_item_rating(self, similarity_list):

        predicted_rating = 0.0
        num, denom = 0.0,0.0
        for similarity, rating in similarity_list:
            num   += similarity * rating
            denom += abs(similarity)

        if denom == 0:
            return self.total_average_rating
        
        predicted_rating = num / denom

        predicted_rating = max(0, min(5, predicted_rating))

        return predicted_rating

    def create_rating_prediction(self, user_id, biz_id):

        #edge case 1 : user_id and biz_id both do not exist
        # return half rating (neutral).
        if (user_id not in self.user_biz_rating_map) and ( biz_id not in self.biz_user_rating_map):
            return self.total_average_rating

        
        #edge case 2 : user_id exists but biz_id does not.
        if user_id not in self.user_biz_rating_map:
            return self.business_average_rating.get(biz_id, self.total_average_rating)

        #edge case 3 : user_id exists but biz_id does not.

        if biz_id not in self.biz_user_rating_map:
            return self.user_average_rating.get(user_id, self.total_average_rating)

        similarity_list = []

        # what if the user has already rated that business?

        for business, rating in self.user_biz_rating_map.get(user_id):
            pearson_similarity  = self.evaluate_pearson_similarity(biz_id, business)
            if pearson_similarity <=0:
                continue
            similarity_list.append((pearson_similarity, rating))

        predicted_rating = self.predict_item_rating(similarity_list)

        return predicted_rating


    def create_predictions_for_test_data(self, test_data_rdd):
    
        # lets have a separate rdd for the ratings column and remove it from the test_data_rdd
        test_data_results          = test_data_rdd.map(lambda row : (row[0], row[1], self.create_rating_prediction(row[0], row[1]))).collect()
                                        
        #print(self.calculateRMSE(test_data_results))
        return test_data_results

    def calculateRMSE(self, test_ratings_data):

        rmse_diff = 0
        for i in range(len(test_ratings_data)):
            rmse_diff += (test_ratings_data[i][0] - test_ratings_data[i][1]) ** 2
        
        return math.sqrt(rmse_diff / len(test_ratings_data))

    def calculate_default_averages(self, training_data_rdd):

        # Average based on user rating when a business from test data is not found in training data
        self.user_average_rating    = training_data_rdd.map(lambda row: (row[0], (row[2], 1)))\
                                                 .reduceByKey(lambda row1, row2: (row1[0] + row2[0], row1[1] + row2[1]))\
                                                 .map(lambda row: (row[0], row[1][0] / row[1][1]))\
                                                 .collectAsMap()
        # Average based on business rating when a user from test data is not found in training data
        self.business_average_rating = training_data_rdd.map(lambda row: (row[1], (row[2], 1)))\
                                                 .reduceByKey(lambda row1, row2: (row1[0] + row2[0], row1[1] + row2[1]))\
                                                 .map(lambda row: (row[0], row[1][0] / row[1][1]))\
                                                 .collectAsMap()

        # Average of all ratings in data

        self.total_average_rating_rdd = training_data_rdd.map(lambda row : (row[2], 1))\
                                                .reduce(lambda row1, row2 : (row1[0] + row2[0], row1[1] + row2[1]))\
        
        self.total_average_rating     = self.total_average_rating_rdd[0] / self.total_average_rating_rdd[1]    

    def write_results_to_csv(self, test_results, output_file):
        with open(output_file, 'w+') as f:
            writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
            writer.writerow(['user_id',' business_id', ' prediction'])
            for result in test_results:
                writer.writerow([result[0], result[1], result[2]])



    def task2Point1(self):
        start_time = time()
        sc                                            = self.setup()
        training_data_rdd, test_data_rdd, output_file = self.read_and_pre_process_input_file(sc)
        self.calculate_default_averages(training_data_rdd)
        # create user indexed dictionary and business indexed dictionary
        self.generate_training_data_maps(training_data_rdd)

        # start predictions on validation data

        test_results = self.create_predictions_for_test_data(test_data_rdd)
        self.write_results_to_csv(test_results, output_file)
        print("Duration: " + str(time() - start_time))

    def task2Point3(self, sc, training_file, test_file):

        training_data_rdd = self.create_rdd_from_file(sc, training_file).map(lambda row : (row[0], row[1], float(row[2])))
        test_data_rdd     = self.create_rdd_from_file(sc, test_file).map(lambda row : (row[0], row[1]))

        self.calculate_default_averages(training_data_rdd)
        # create user indexed dictionary and business indexed dictionary
        self.generate_training_data_maps(training_data_rdd)

        # start predictions on validation data

        return self.create_predictions_for_test_data(test_data_rdd)



def main():
    t1 = Task2Point1()
    t1.task2Point1()
        

if __name__ == '__main__':
    main()