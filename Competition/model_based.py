from pyspark     import SparkContext, SparkConf
import sys
import numpy   as np
import xgboost as xgb
import json
from time import time
import csv
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


business_attributes_bool   = ['BusinessAcceptsCreditCards', 'BikeParking', 'GoodForKids', 'RestaurantsTakeOut', 'OutdoorSeating', 'RestaurantsGoodForGroups', 'WheelchairAccessible', 'RestaurantsDelivery', 'RestaurantsReservations', 'HasTV', 'ByAppointmentOnly']
business_attributes_num    = ['RestaurantsPriceRange2']
restaurant_attire_map      = {'casual' : 1, 'formal' : 2, 'dressy' : 3}
photo_labels_map           = {'food' : 0, 'drink' : 1, 'inside' : 2, 'outside' : 3, 'menu' : 4}
generic_user_avg           = [3.75117, 1.03298, 0.33442, -0.70884, 5, 1]
generic_biz_avg            = [3.75088, 0.990780, 0.48054, -0.70888, 5, 1]
train_cols                 = ['uid', 'bid', 'user_average_stars', 'user_review_count', 'user_funny', 'user_useful', 'user_fans', 'user_cool', 'biz_stars', 'biz_review_count', 'biz_lat', 'biz_long', 'biz_open', 'BusinessAcceptsCreditCards', 'BikeParking', 'GoodForKids', 'RestaurantsTakeOut', 'OutdoorSeating', 'RestaurantsGoodForGroups', 'WheelchairAccessible', 'RestaurantsDelivery', 'RestaurantsReservations', 'HasTV', 'ByAppointmentOnly', 'RestaurantsPriceRange2', 'BusinessParking', 'Alcohol', 'Ambience', 'NoiseLevel', 'RestaurantAttire', 'WiFi', 'GoodForMeal', 'photo_food', 'photo_drink', 'photo_inside', 'photo_outside', 'photo_menu', 'tips_biz', 'tips_user', 'tip_sentiment', 'biz_avg_checkins', 'user_mean', 'user_std', 'user_kurt', 'user_skew', 'user_max', 'user_min', 'biz_mean', 'biz_std', 'biz_kurt', 'biz_skew', 'biz_max', 'biz_min']
test_cols                  = ['uid', 'bid', 'user_average_stars', 'user_review_count', 'user_funny', 'user_useful', 'user_fans', 'user_cool', 'biz_stars', 'biz_review_count', 'biz_lat', 'biz_long', 'biz_open', 'BusinessAcceptsCreditCards', 'BikeParking', 'GoodForKids', 'RestaurantsTakeOut', 'OutdoorSeating', 'RestaurantsGoodForGroups', 'WheelchairAccessible', 'RestaurantsDelivery', 'RestaurantsReservations', 'HasTV', 'ByAppointmentOnly', 'RestaurantsPriceRange2', 'BusinessParking', 'Alcohol', 'Ambience', 'NoiseLevel', 'RestaurantAttire', 'WiFi', 'GoodForMeal', 'photo_food', 'photo_drink', 'photo_inside', 'photo_outside', 'photo_menu', 'tips_biz', 'tips_user', 'tip_sentiment', 'biz_avg_checkins', 'user_mean', 'user_std', 'user_kurt', 'user_skew', 'user_max', 'user_min', 'biz_mean', 'biz_std', 'biz_kurt', 'biz_skew', 'biz_max', 'biz_min']
business_attr_cols         = ['BusinessAcceptsCreditCards', 'BikeParking', 'GoodForKids', 'RestaurantsTakeOut', 'OutdoorSeating', 'RestaurantsGoodForGroups', 'WheelchairAccessible', 'RestaurantsDelivery', 'RestaurantsReservations', 'HasTV', 'ByAppointmentOnly', 'RestaurantsPriceRange2', 'BusinessParking', 'Alcohol', 'Ambience', 'NoiseLevel', 'RestaurantAttire', 'WiFi', 'GoodForMeal']

class ModelBased():

    def setup(self):
        conf = SparkConf().setAppName('DM_competition_model_based').setMaster('local[*]')
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


    def add_user_features(self, og_data, features, user_data_map):
        # only add user_id, business_id and user_data first to all features
        for i, row in enumerate(og_data):
            user_id     = row[0]
            business_id = row[1]
            features[i].extend([user_id, business_id])
            if user_id in user_data_map:
                features[i].extend(user_data_map.get(user_id))
            else:
                features[i].extend([None, None, None, None, None, None])
        return features

    def add_business_features(self, features, business_data_map, business_attributes_map):
        for i, row in enumerate(features):
            business_id = row[1]
            if business_id in business_data_map:
            # business_attributes_map is {bid : {attr1 : v1, attr2 : v2}}
                features[i].extend(business_data_map.get(business_id))
                attr = business_attributes_map.get(business_id)
                if attr:
                    for key in business_attributes_bool:
                        features[i].append(int(bool(attr[key]))) if key in attr else features[i].append(0)
                    for key in business_attributes_num:
                        features[i].append(int(attr[key])) if key in attr else features[i].append(0)
                    
                    #Business Parking
                    features[i].append(attr.get('BusinessParking').count('True')) if attr.get('BusinessParking') else features[i].append(0)
                    #Alcohol
                    features[i].append(1) if attr.get('Alcohol') and attr.get('Alcohol') != 'none' else features[i].append(0)
                    #Ambience
                    features[i].append(attr.get('Ambience').count('True')) if attr.get('Ambience') else features[i].append(0)
                    #Noise Level
                    features[i].append(1) if attr.get('NoiseLevel') and attr.get('NoiseLevel') == 'average' else features[i].append(0)
                    # Restaurants Attire
                    features[i].append(restaurant_attire_map.get(attr.get('RestaurantAttire'))) if attr.get('RestaurantAttire') and attr.get('RestaurantAttire') != 'none' else features[i].append(0)
                    # Wifi
                    features[i].append(1) if attr.get('WiFi') == 'Free' else features[i].append(0)
                    #GoodForMeal
                    features[i].append(attr.get('GoodForMeal').count('True')) if attr.get('GoodForMeal') else features[i].append(0)
                     
                else:
                    features[i].extend([None] * (len(business_attributes_bool) + len(business_attributes_num) + 7))
                        
            else:
                features[i].extend([None] * (5 + 7 + len(business_attributes_bool) + len(business_attributes_num)))
            
        return features

    def add_photo_features(self, features, photos_data_map):
        # Add photo labels data to features
        for i, row in enumerate(features):

            # food, drink, inside, outside, menu.
            photos_labels_list   = [0] * 5
            business_id = row[1]
            if business_id in photos_data_map:
                idx = photo_labels_map.get(photos_data_map.get(business_id))
                photos_labels_list[idx] += 1
        
            features[i].extend(photos_labels_list)
        return features

    def add_tip_features(self, features, tip_bix_data_map, tip_user_data_map, tip_sentiment_map):
        # Add no. of tips per business/user data to features
        for i, row in enumerate(features):
            user_id     = row[0]
            business_id = row[1]
            features[i].append(tip_bix_data_map.get(business_id)) if tip_bix_data_map.get(business_id) else features[i].append(0)
            features[i].append(tip_user_data_map.get(user_id)) if tip_user_data_map.get(user_id) else features[i].append(0)
            features[i].append(tip_sentiment_map.get((user_id, business_id))) if tip_sentiment_map.get((user_id, business_id)) else features[i].append(0)
        return features

    def add_checkin_features(self, features, checkin_data_map):
        # Add avg number of checkins data to features
        for i, row in enumerate(features):
            business_id = row[1]
            features[i].append(checkin_data_map.get(business_id)) if checkin_data_map.get(business_id) else features[i].append(0)

        return features

    def create_statistics(self, ratings_agg, generic_avg):
        # avg, std, kurt, skew, max, min.
        stats = []
        ratings_agg = list(map(float, ratings_agg))
        if ratings_agg and len(ratings_agg) > 0:
            ratings_agg_series = pd.Series(ratings_agg)
            stats.append(ratings_agg_series.mean())
            stats.append(ratings_agg_series.std())
            stats.append(ratings_agg_series.kurt())
            stats.append(ratings_agg_series.skew())
            stats.append(ratings_agg_series.max())
            stats.append(ratings_agg_series.min())
            return stats
        else:
            return generic_avg

    def create_and_add_statistics(self, row, user_ratings_aggregated, biz_ratings_aggregated, data_type='train'):

        if data_type == 'train':
            feature = row[0]
            training_data = row[1]
            user_id = feature[0]
            biz_id  = feature[1]
            rating  = training_data[2]
            ratings_agg = user_ratings_aggregated.get(user_id, [])
            ratings_agg.remove(rating) if ratings_agg != [] else ratings_agg
            user_stats = self.create_statistics(ratings_agg, generic_user_avg)

            feature.extend(user_stats)
            ratings_agg = biz_ratings_aggregated.get(biz_id, [])
            ratings_agg.remove(rating) if ratings_agg != [] else ratings_agg
            biz_stats = self.create_statistics(ratings_agg, generic_biz_avg)
            feature.extend(biz_stats)
        else:
            feature = row
            user_id = feature[0]
            biz_id  = feature[1]
            user_ratings_agg = user_ratings_aggregated.get(user_id, [])
            biz_ratings_agg  = biz_ratings_aggregated.get(biz_id, [])
            user_ratings_agg = [vector[1] for vector in user_ratings_agg if vector[0] != biz_id]
            biz_ratings_agg  = [vector[1] for vector in biz_ratings_agg if vector[0] != user_id]
            user_stats       = self.create_statistics(user_ratings_agg, generic_user_avg)
            biz_stats        = self.create_statistics(biz_ratings_agg, generic_biz_avg)
            feature.extend(user_stats)
            feature.extend(biz_stats)

        return feature

    def add_statistics_features(self, data_features, train_data_rdd, sc, test_data_rdd = None, data_type = 'train', training_data = None):

        # First lets create 2 aggregated RDD's for user ratings and business ratings.
        if data_type == 'train':
            user_ratings_aggregated = train_data_rdd.map(lambda row : (row[0], row[2])).groupByKey().mapValues(list).collectAsMap()
            biz_ratings_aggregated  = train_data_rdd.map(lambda row : (row[1], row[2])).groupByKey().mapValues(list).collectAsMap()
            data_features           = sc.parallelize(list(zip(data_features, training_data))).map(lambda row : self.create_and_add_statistics(row, user_ratings_aggregated, biz_ratings_aggregated, 'train')).collect()
            
        else:
            user_ratings_aggregated = train_data_rdd.map(lambda row : (row[0], (row[1], row[2]))).groupByKey().mapValues(list).collectAsMap()
            biz_ratings_aggregated  = train_data_rdd.map(lambda row : (row[1], (row[0],row[2]))).groupByKey().mapValues(list).collectAsMap()
            data_features           = sc.parallelize(data_features).map(lambda row : self.create_and_add_statistics(row, user_ratings_aggregated, biz_ratings_aggregated, 'test')).collect()
        
        return data_features

    def enhance_pd(self, features_pd, data_type):

        biz_attr_pd = features_pd[business_attr_cols]

        drop = True if data_type == 'train' else False
        for column in biz_attr_pd.columns:
            tmp_data         = pd.get_dummies(features_pd[column].fillna('NA'), drop_first=drop)
            new_columns = []
            for col_i in tmp_data.columns:
                new_columns.append(column + str(col_i))
            tmp_data.columns = new_columns
            features_pd.drop(column, axis=1, inplace=True)
            features_pd = pd.concat([features_pd, tmp_data], axis=1)
        
        stats_features = ['biz_mean', 'biz_std', 'biz_kurt', 'biz_skew', 'biz_max', 'biz_min']

        for i in range(len(stats_features)):
            features_pd[stats_features[i]].fillna(generic_biz_avg[i], inplace=True)
        features_pd.fillna(0, inplace=True)

        return features_pd

    def convert_to_pd(self, features, cols, data_type='train'):

        features_pd = pd.DataFrame(features, columns=cols)
        features_pd = self.enhance_pd(features_pd, data_type)

        return features_pd

    def find_sentiment_value(self, row, sentObj):
        
        textList = row
        sentiment_results = 0

        for text in textList:
            sentiment_result_dict = sentObj.polarity_scores(text)
            sentiment_results += sentiment_result_dict['compound']

        return sentiment_results / len(textList)

    def read_and_pre_process_input_file(self, sc, training_folder_path, test_file):

        training_file_path = training_folder_path + "/yelp_train.csv"
        user_file_path     = training_folder_path + "/user.json"
        business_file_path = training_folder_path + "/business.json"
        photo_file_path    = training_folder_path + "/photo.json"
        tip_file_path      = training_folder_path + "/tip.json"
        checkin_file_path  = training_folder_path + "/checkin.json"

        sentObj            = SentimentIntensityAnalyzer()

        labels_data            = []
        training_data_rdd      = self.create_rdd_from_file(sc, training_file_path)
        training_data          = training_data_rdd.collect()
        test_data_rdd          = self.create_rdd_from_file(sc, test_file)
        is_val_data_check      = test_data_rdd.take(1)
        if len(is_val_data_check[0]) == 3:
            labels_data   = test_data_rdd.map(lambda row : row[2]).collect()
        test_data              = test_data_rdd.collect()

        training_data_features = [[] for _ in range(len(training_data))]
        test_data_features     = [[] for _ in range(len(test_data))]

        user_data_rdd     = self.load_json_rdd(sc, user_file_path)
        user_data_map     = user_data_rdd.map(lambda row : (row['user_id'], (row['average_stars'], row['review_count'], row['funny'], row['useful'], row['fans'], row['cool'])))\
                                         .collectAsMap()

        training_data_features = self.add_user_features(training_data, training_data_features, user_data_map)
        test_data_features     = self.add_user_features(test_data, test_data_features, user_data_map)

        del user_data_map
        del user_data_rdd
        

        # add business_data to features
        business_data_rdd = self.load_json_rdd(sc, business_file_path)
        business_data_map = business_data_rdd.map(lambda row : (row['business_id'], (row['stars'], row['review_count'], row['latitude'], row['longitude'], row['is_open'])))\
                                             .collectAsMap()
        business_attributes_map = business_data_rdd.map(lambda row : (row['business_id'], row['attributes'])).collectAsMap()

        training_data_features  = self.add_business_features(training_data_features, business_data_map, business_attributes_map)
        test_data_features      = self.add_business_features(test_data_features, business_data_map, business_attributes_map)

        del business_attributes_map
        del business_data_rdd
        del business_data_map

        photos_data_rdd   = self.load_json_rdd(sc, photo_file_path)
        photos_data_map   = photos_data_rdd.map(lambda row : (row['business_id'], row['label'])).collectAsMap()

        training_data_features = self.add_photo_features(training_data_features, photos_data_map)
        test_data_features     = self.add_photo_features(test_data_features, photos_data_map)
        
        del photos_data_map
        del photos_data_rdd

        tip_data_rdd          = self.load_json_rdd(sc, tip_file_path)
        # No . of tips per user and per business.
        tip_bix_data_map      = tip_data_rdd.map(lambda row : (row['business_id'], 1)).reduceByKey(lambda row1, row2 : row1 + row2).collectAsMap()
        tip_user_data_map     = tip_data_rdd.map(lambda row : (row['user_id'], 1)).reduceByKey(lambda row1, row2 : row1 + row2).collectAsMap()

        tip_sentiment_map     = tip_data_rdd.map(lambda row : ((row['user_id'], row['business_id']), row['text'])).groupByKey().mapValues(lambda row : self.find_sentiment_value(row, sentObj)).collectAsMap()

        training_data_features = self.add_tip_features(training_data_features, tip_bix_data_map, tip_user_data_map, tip_sentiment_map)
        test_data_features     = self.add_tip_features(test_data_features, tip_bix_data_map, tip_user_data_map, tip_sentiment_map)
        
        del tip_bix_data_map
        del tip_user_data_map
        del tip_data_rdd


        checkin_data_rdd  = self.load_json_rdd(sc, checkin_file_path)
        checkin_data_map  = checkin_data_rdd.map(lambda row : (row['business_id'], (sum(row['time'].values()), len(row['time'].values())))).map(lambda row : (row[0], (row[1][0] / row[1][1]))).collectAsMap()

        training_data_features = self.add_checkin_features(training_data_features, checkin_data_map)
        test_data_features     = self.add_checkin_features(test_data_features, checkin_data_map)
        
        del checkin_data_map
        del checkin_data_rdd

        training_data_features = self.add_statistics_features(training_data_features, training_data_rdd, sc, test_data_rdd=None, data_type='train', training_data = training_data)


        test_data_features     = self.add_statistics_features(test_data_features, training_data_rdd, sc, test_data_rdd=test_data_rdd, data_type='test', training_data = None)


        ratings = []
        for i in range(len(training_data)):
            ratings.append(training_data[i][2])
        
        training_data_pd = self.convert_to_pd(training_data_features, train_cols)
        test_data_pd     = self.convert_to_pd(test_data_features, test_cols)

        training_data_pd['rating'] = ratings
        return training_data_pd, test_data_pd, labels_data
       
    def create_and_split_training_data(self, training_data):

        training_data   = training_data.to_numpy()
        training_data_x = np.array(training_data[:, 2: -1], dtype='float')
        training_data_y = np.array(training_data[:, -1], dtype='float')

        return training_data_x, training_data_y

    def apply_XGBRegressor(self, training_data_x, training_data_y, test_data_x):

        model          = xgb.XGBRegressor(objective = 'reg:linear', verbosity = 0, learning_rate = 0.1, max_depth = 7, n_estimators = 100, subsample = 0.5, random_state = 1, gamma = 0.5)
        model.fit(training_data_x, training_data_y)
        return model.predict(test_data_x)

    def write_results_to_csv(self, results, output_file):
        with open(output_file,'w+') as f:
            writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
            writer.writerow(['user_id',' business_id', ' prediction'])
            for result in results:
                writer.writerow(result)


    def gather_results(self, test_data, predicted_rating):
        return [(test_data[i][0], test_data[i][1], predicted_rating[i]) for i in range(len(test_data))]

    def model(self, sc, training_folder_path, test_file):
        training_data, test_data, labels_data = self.read_and_pre_process_input_file(sc, training_folder_path, test_file)
        training_data_x, training_data_y      = self.create_and_split_training_data(training_data)
        test_data          = np.array(test_data)
        test_data_x        = np.array(test_data[:, 2:], dtype='float')
        predicted_rating   = self.apply_XGBRegressor(training_data_x, training_data_y, test_data_x)
        if labels_data:
            return [(test_data[i][0], test_data[i][1], predicted_rating[i], labels_data[i]) for i in range(len(test_data))]
        else:
            return self.gather_results(test_data, predicted_rating)





def main():
    model_based = ModelBased()
    model_based.model()
        
if __name__ == '__main__':
    main()