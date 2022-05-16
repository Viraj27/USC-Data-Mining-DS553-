"""
Method Description:

I have adopted a purely model based recommendation approach fine-tuning the XGBRegressor prediction model for this problem.
A lot of feature engineering has been incorporated for this solution. New features which focus around the statistics of the 
aggregated ratings for each user_id and each business_id have also been created and that has shown to boost ratings prediction.
Along with that, sentiment analysis on the tips given by users for the particular business has also been performed to provide a relation between the user sentiment and the rating.
Features selected for this problem are as follows:

user.json - average_stars, review_count, funny, useful, cool, fans.

business.json - stars, review_count, latitude, longitude, is_open, attributes = ['BusinessAcceptsCreditCards', 'BikeParking', 'GoodForKids', 'RestaurantsTakeOut',\
                'OutdoorSeating', 'RestaurantsGoodForGroups', 'WheelchairAccessible', 'RestaurantsDelivery', 'RestaurantsReservations', 'HasTV', 'ByAppointmentOnly', 'RestaurantsPriceRange2', \
                'BusinessParking', 'Alcohol', 'Ambience', 'NoiseLevel', 'RestaurantAttire', 'WiFi', 'GoodForMeal']

photo.json - count of labels [food, drink, inside, outside, menu].

tip.json - The number of tips given for each user and for each business id. Sentiment score for each user_id, business_id pair.

checkin.json - average number of checkins per business.

New features - For each user and for each business, the following statistical features regarding the aggregated ratings for each user and business in the training data were added:

The generic_user_avg and generic_biz_avg list values are the statistical measurements on the entire dataset that have been used as default values in case user_id, business_id pair keys are unavailable.

mean - mean rating per user_id and business_id.
std  - standard deviation per user_id and business_id.
kurt - returns unbiased kurtosis over requested axis.
skew - returns unbiased skew over requested axis.
max  - maximum rating per user_id and business_id.
min  - minimum rating per user_id and business_id.

The XGB regressor model has been used with the following hyperparameters:

objective = 'reg:linear', verbosity = 0, learning_rate = 0.1, max_depth = 7, n_estimators = 100, subsample = 0.5, gamma = 0.5, random_state = 1

Error Distribution:

RMSE:
0.9728363672008816

Execution Time:
585.6767604351044

"""

from pyspark     import SparkContext, SparkConf
from model_based import ModelBased
import sys
import csv
import math
from time import time
import numpy as np


class Competition():

    def setup(self):
        conf = SparkConf().setAppName('DM_competition_model_based').setMaster('local[*]')
        sc   = SparkContext.getOrCreate(conf = conf)

        sc.setSystemProperty('spark.driver.memory', '4g')
        sc.setSystemProperty('spark.executor.memory', '4g')
        sc.setLogLevel('ERROR')

        training_folder_path, test_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
        training_file = training_folder_path + "/yelp_train.csv"

        return sc, training_folder_path, training_file, test_file, output_file

    def calculateRMSE(self, predicted_rating, model_pred):
        og_ratings_list                 = []
        for i in range(len(model_pred)):
            og_ratings_list.append(model_pred[i][3])
        num, denom = 0,0 
        for i in range(len(predicted_rating)):
            num += (float(predicted_rating[i]) - float(og_ratings_list[i])) ** 2
            denom += 1
        print((num/denom)** 0.5)

    def write_results_to_csv(self, test_results, predicted_rating, output_file):
            with open(output_file, 'w+') as f:
                writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
                writer.writerow(['user_id',' business_id', ' prediction'])
                for i in range(len(test_results)):
                    writer.writerow([test_results[i][0], test_results[i][1], predicted_rating[i]])

    def competition(self):
        start_time = time()
        sc, training_folder_path, training_file, test_file, output_file = self.setup()
        model_based = ModelBased()
        xgb_regressor_based_predictions = model_based.model(sc, training_folder_path, test_file) 
        predicted_ratings_list          = []
        for i in range(len(xgb_regressor_based_predictions)):
            predicted_ratings_list.append(xgb_regressor_based_predictions[i][2])
        if len(xgb_regressor_based_predictions[0]) == 4:
            self.calculateRMSE(predicted_ratings_list, xgb_regressor_based_predictions)
        self.write_results_to_csv(xgb_regressor_based_predictions, predicted_ratings_list, output_file)
        print("Duration: " + str(time() - start_time))


def main():
    comp = Competition()
    comp.competition()
    

if __name__ == '__main__':
    main()