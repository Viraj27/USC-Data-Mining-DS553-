from pyspark     import SparkContext, SparkConf
import sys
import numpy   as np
import xgboost as xgb
import json
from time import time
import csv

class Task2Point2():

    def setup(self):
        conf = SparkConf().setAppName('DM_assign3_task2point2').setMaster('local[*]')
        sc   = SparkContext.getOrCreate(conf = conf)

        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        sc.setLogLevel('ERROR')

        return sc

    def load_json_rdd(self, sc, file):

        rdd = sc.textFile(file).map(lambda row : json.loads(row))

        return rdd

    def create_rdd_from_file(self, sc, file):

        rdd                = sc.textFile(file).map(lambda row : row.split(','))
        csv_header_data    = rdd.first()
        rdd                = rdd.filter(lambda row : row != csv_header_data)

        return rdd

    def add_features_to_rdd(self, row, user_data_map, business_data_map):
        
        rating = 0

        user_id, business_id = row[0], row[1]

        feature_set = [user_id, business_id]

        if user_id in user_data_map:
            feature_set.extend(user_data_map.get(user_id))
        else:
            feature_set.extend([None, None])

        if business_id in business_data_map:
            feature_set.extend(business_data_map.get(business_id))
        else:
            feature_set.extend([None, None])
        
        if len(row) == 3:
            rating = float(row[2])
        
        feature_set.append(rating)

        return feature_set

    def read_and_pre_process_input_file(self, sc, training_folder_path, test_file):

        training_file_path = training_folder_path + "/yelp_train.csv"
        user_file_path     = training_folder_path + "/user.json"
        business_file_path = training_folder_path + "/business.json"

        user_data_rdd     = self.load_json_rdd(sc, user_file_path)
        user_data_map     = user_data_rdd.map(lambda row : (row['user_id'], (row['average_stars'], row['review_count'])))\
                                         .collectAsMap()

        business_data_rdd = self.load_json_rdd(sc, business_file_path)
        business_data_map = business_data_rdd.map(lambda row : (row['business_id'], (row['stars'], row['review_count'])))\
                                             .collectAsMap()

        training_data_rdd = self.create_rdd_from_file(sc, training_file_path)
        training_data_rdd = training_data_rdd.map(lambda row : self.add_features_to_rdd(row, user_data_map, business_data_map))

        test_data_rdd     = self.create_rdd_from_file(sc, test_file)
        test_data_rdd     = test_data_rdd.map(lambda row : self.add_features_to_rdd(row, user_data_map, business_data_map))

        return training_data_rdd.collect(), test_data_rdd.collect()


    def create_and_split_training_data(self, training_data):

        training_data   = np.array(training_data)
        training_data_x = np.array(training_data[:, 2: -1], dtype='float')
        training_data_y = np.array(training_data[:, -1], dtype='float')
        return training_data_x, training_data_y

    def apply_XGBRegressor(self, training_data_x, training_data_y, test_data_x):

        model          = xgb.XGBRegressor(objective = 'reg:linear')
        model.fit(training_data_x, training_data_y)
        return model.predict(test_data_x)

    # def calculateRMSE(self, predicted_rating, test_data):
        
    #     num, denom = 0,0 
    #     for i in range(len(predicted_rating)):
    #         num += (float(predicted_rating[i]) - float(test_data[i][-1])) ** 2
    #         denom += 1
        
    #     print((num/denom)** 0.5)

    def write_results_to_csv(self, results, output_file):
        with open(output_file,'w+') as f:
            writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
            writer.writerow(['user_id',' business_id', ' prediction'])
            for result in results:
                writer.writerow(result)


    def gather_results(self, test_data, predicted_rating):
        # results = []
        # for i in range(len(test_data)):
        #     results.append((test_data[i][0], test_data[i][1], predicted_rating[i]))
        # return results
        return [(test_data[i][0], test_data[i][1], predicted_rating[i]) for i in range(len(test_data))]

    def task2Point2(self):

        start_time                                   = time()
        sc                                           = self.setup()
        training_folder_path, test_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
        training_data, test_data = self.read_and_pre_process_input_file(sc, training_folder_path, test_file)
        training_data_x, training_data_y      = self.create_and_split_training_data(training_data)
        test_data          = np.array(test_data)
        test_data_x        = np.array(test_data[:, 2:-1], dtype='float')
        predicted_rating   = self.apply_XGBRegressor(training_data_x, training_data_y, test_data_x)
        results_list       = self.gather_results(test_data, predicted_rating)
        #self.calculateRMSE(predicted_rating, test_data)
        self.write_results_to_csv(results_list, output_file)
        print("Duration: " + str(time() - start_time))
        

    def task2Point3(self, sc, training_folder_path, test_file):
        training_data, test_data              = self.read_and_pre_process_input_file(sc, training_folder_path, test_file)
        training_data_x, training_data_y      = self.create_and_split_training_data(training_data)
        test_data          = np.array(test_data)
        test_data_x        = np.array(test_data[:, 2:-1], dtype='float')
        predicted_rating   = self.apply_XGBRegressor(training_data_x, training_data_y, test_data_x)
        return [(test_data[i][0], test_data[i][1], predicted_rating[i], float(test_data[i][-1])) for i in range(len(test_data))]





def main():
    t1 = Task2Point2()
    t1.task2Point2()
        
if __name__ == '__main__':
    main()