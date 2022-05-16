
from pyspark     import SparkContext, SparkConf
from task2_1     import Task2Point1
from task2_2     import Task2Point2
import sys
import csv
from time import time


class Task2Point3():

    def setup(self):
        conf = SparkConf().setAppName('DM_assign3_task1').setMaster('local[*]')
        sc   = SparkContext.getOrCreate(conf = conf)

        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        sc.setLogLevel('ERROR')

        training_folder_path, test_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
        training_file = training_folder_path + "/yelp_train.csv"

        return sc, training_folder_path, training_file, test_file, output_file

    def write_results_to_csv(self, test_results, predicted_rating, output_file):
            with open(output_file, 'w+') as f:
                writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
                writer.writerow(['user_id',' business_id', ' prediction'])
                for i in range(len(test_results)):
                    writer.writerow([test_results[i][0], test_results[i][1], predicted_rating[i]])



    def task2Point3(self):
        start_time = time()
        sc, training_folder_path, training_file, test_file, output_file = self.setup()
        t21 = Task2Point1()
        t22 = Task2Point2()
        item_based_cf_predictions       = t21.task2Point3(sc, training_file, test_file)
        xgb_regressor_based_predictions = t22.task2Point3(sc, training_folder_path, test_file) 
        predicted_ratings_list          = []
        alpha = 0.01
        num, denom = 0.0, 0.0
        for i in range(len(xgb_regressor_based_predictions)):
            predicted_ratings_list.append((alpha * item_based_cf_predictions[i][2]) +((1-alpha) * xgb_regressor_based_predictions[i][2]))

        self.write_results_to_csv(xgb_regressor_based_predictions, predicted_ratings_list, output_file)
        #print(math.sqrt(num/denom))
        print("Duration: " + str(time() - start_time))


def main():
    t23 = Task2Point3()
    t23.task2Point3()
    

if __name__ == '__main__':
    main()