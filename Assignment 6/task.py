import random
from sklearn.cluster import KMeans
import sys
import numpy as np
from collections import defaultdict
import itertools
import random
import copy
import math
import sys

class Cluster():
    def __init__(self, points_list):
        self.points_list               = points_list
        features_data                  = list(map(lambda x : x[2:], self.points_list))
        self.n                         = len(self.points_list)
        self.sum_list                  = [sum(x) for x in zip(*features_data)]
        sum_square_nested_list         = [[sum(a**2 for a in x)]for x in zip(*features_data)]
        self.sum_square_list           = list(itertools.chain(*sum_square_nested_list))
        self.centroid                  = [x / self.n for x in self.sum_list]
        self.standard_deviation        = [math.sqrt((self.sum_square_list[i] / self.n) - self.centroid[i] ** 2) for i in range(len(self.sum_square_list))]
    
    def add_point(self, point):
        self.points_list.append(point)
        features_data = point[2:]
        self.n        += 1
        self.sum_list            = [sum(x) for x in zip(self.sum_list, features_data)]
        self.sum_square_list     = [x[0] + x[1]**2 for x in zip(self.sum_square_list, features_data)]
        self.centroid            = [x / self.n for x in self.sum_list]
        self.standard_deviation  = [math.sqrt((self.sum_square_list[i] / self.n) - self.centroid[i] ** 2) for i in range(len(self.sum_square_list))]
        
def main():

    #input_file, num_clusters, output_file = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    
    input_file      = sys.argv[1]
    output_file     = sys.argv[3]
    num_clusters    = int(sys.argv[2])
    input_data      = []
    retained_list, discard_list, compression_list    = list(), list(), list()

    # read the dataset
    with open(input_file, 'r') as f:
        for data in f.readlines():
            # input data in ordered form with the right indices.
            input_data.append(list(map(float, data.rstrip("\n").split(','))))

    dataset_len       = len(input_data)
    random_chunk_size = math.ceil(0.2 * dataset_len) 
    train_data        = []

    # step 1. generate 20% of data randomly.
    random.shuffle(input_data)
    
    train_data = input_data[:random_chunk_size]

    # step 2. K-means
    
    features_data = list(map(lambda x : x[2:], train_data))

    k_means_fitted_data = KMeans(num_clusters * 10).fit(features_data)

    cluster_points      = k_means_fitted_data.labels_
    
    cluster_points_list = cluster_points.tolist()
    
    cluster_point_map   = defaultdict(list)

    for idx, cluster in enumerate(cluster_points_list):
        cluster_point_map[cluster].append(train_data[idx])
    
    # step 3. Move all clusters with one point to RS.
    remaining_data_points = []
    for cluster, points_set in cluster_point_map.items():
        if len(points_set) == 1:
            # RS list
            retained_list.append(points_set[0])
        else:
            # Remaining data
            remaining_data_points += points_set

    # step 4. Do K-means again on rest of data points with k = num_clusters.
    
    features_data = list(map(lambda x : x[2:], remaining_data_points))
    
    k_means_fitted_remaining_data = KMeans(num_clusters).fit(features_data)
    
    #step 5. Create DS.
    cluster_points      = k_means_fitted_remaining_data.labels_
    cluster_points_list = cluster_points.tolist()
    
    cluster_point_map   = defaultdict(list)
    
    # group all points in cluster
    for idx, cluster in enumerate(cluster_points_list):
        cluster_point_map[cluster].append(remaining_data_points[idx])
    
    #N: The number of points
    #SUM: the sum of the coordinates of the points
    #SUMSQ: the sum of squares of coordinates
    
    for cluster, points_list in cluster_point_map.items():
        discard_list.append(Cluster(points_list))

    with open(output_file, 'w+') as f:
        discard_num_points = sum([cluster.n for cluster in discard_list])
        compression_num_points = sum([cluster.n for cluster in compression_list])
        f.write("The intermediate results:\n")
        s = ','.join([str(discard_num_points), str(len(compression_list)), str(compression_num_points), str(len(retained_list))])
        f.write("Round 1: " + s + "\n")
    # step 6 : 
    #ret_saved_list = copy.deepcopy(retained_list)
    features_data = list(map(lambda x : x[2:], retained_list))
    if (num_clusters * 10) < len(features_data):
        k_means_fitted_retained_data = KMeans(num_clusters * 10).fit(features_data)
        #retained_list = []
        cluster_points      = k_means_fitted_retained_data.labels_
        cluster_points_list = cluster_points.tolist()
        cluster_point_map   = defaultdict(list)
        for idx, cluster in enumerate(cluster_points_list):
            cluster_point_map[cluster].append(retained_list[idx])

        retained_set = []
        for cluster, points_set in cluster_point_map.items():
            if len(points_set) == 1:
                # RS
                retained_list.append(points_set[0])
            else:
                # CS
                compression_list.append(Cluster(points_set))
    
    i = 1
    while i < 5:
        
        # step 7 : Load next 20% of data
        train_data = input_data[random_chunk_size * i : random_chunk_size * (i+1)]
        
        #step 8, 9 and 10
    
        for data_point in train_data:
            add_to_discard_list     = False
            add_to_compression_list = False
            min_maha_dist = float('inf')
            min_cluster   = None
            feature_data  = data_point[2:]
            for cluster_obj in discard_list:
                # loop through all clusters - keep track of min maha_dist and that cluster. Add to that cluster.
                maha_dist     = math.sqrt(sum([((feature_data[i] - cluster_obj.centroid[i])/cluster_obj.standard_deviation[i])**2 for i in range(len(feature_data))]))
                if maha_dist < 2 * math.sqrt(len(feature_data)) and maha_dist < min_maha_dist:
                    min_maha_dist = maha_dist
                    min_cluster   = cluster_obj
                    add_to_discard_list = True
            # will have min_maha_dist and min_cluster obj
            if add_to_discard_list:
                min_cluster.add_point(data_point)
            else:
                # try and add to CS.
                for cluster_obj in compression_list:
                    # loop through all clusters - keep track of min maha_dist and that cluster. Add to that cluster.
                    maha_dist     = math.sqrt(sum([((feature_data[i] - cluster_obj.centroid[i])/cluster_obj.standard_deviation[i])**2 for i in range(len(feature_data))]))
                    if maha_dist < 2 * math.sqrt(len(feature_data)) and maha_dist < min_maha_dist:
                        min_maha_dist = maha_dist
                        min_cluster   = cluster_obj
                        add_to_compression_list = True
                if add_to_compression_list:
                    min_cluster.add_point(data_point)
                else:
                    # should I clear retained_list before adding this new set?
                    retained_list.append(data_point)
                    
        # step 11 - run K means on RS to generate RS and CS. 
        #ret_saved_list = copy.deepcopy(retained_list)
        features_data = list(map(lambda x : x[2:], retained_list))
        if (num_clusters * 10) < len(features_data):
            k_means_fitted_retained_data = KMeans(num_clusters * 10).fit(features_data)
            #retained_list = []
            cluster_points      = k_means_fitted_retained_data.labels_
            cluster_points_list = cluster_points.tolist()
            cluster_point_map   = defaultdict(list)
            for idx, cluster in enumerate(cluster_points_list):
                cluster_point_map[cluster].append(retained_list[idx])

            retained_list = []
            for cluster, points_set in cluster_point_map.items():
                if len(points_set) == 1:
                    # RS
                    retained_list.append(points_set[0])
                else:
                    # CS
                    compression_list.append(Cluster(points_set))
        
        # step 12 - Merge CS clusters with maha_dist < 2 * sqrt(d)
        cs_cluster_combos = itertools.combinations(compression_list, 2)
        seen_compression_clusters = []
        for clusters in cs_cluster_combos:
            if clusters[0] in seen_compression_clusters or clusters[1] in seen_compression_clusters:
                continue
            # calculate maha_dist between the centroids.
            cluster_1_centroid = clusters[0].centroid
            cluster_2_centroid = clusters[1].centroid
            maha_dist     = math.sqrt(sum([((cluster_1_centroid[i] - cluster_2_centroid[i])/clusters[0].standard_deviation[i])**2 for i in range(len(cluster_1_centroid))]))
            if maha_dist < 2 * math.sqrt(len(cluster_1_centroid)):
                new_points_list = clusters[0].points_list + clusters[1].points_list
                compression_list.remove(clusters[0])
                compression_list.remove(clusters[1])
                seen_compression_clusters.append(clusters[0])
                seen_compression_clusters.append(clusters[1])
                compression_list.append(Cluster(new_points_list))

        with open(output_file, 'a+') as f:
            discard_num_points = sum([cluster.n for cluster in discard_list])
            compression_num_points = sum([cluster.n for cluster in compression_list])
            s = ','.join([str(discard_num_points), str(len(compression_list)), str(compression_num_points), str(len(retained_list))])
            f.write("Round {0}: ".format(i+1) + s + "\n")
        i += 1
        # Need to reinitialize the sets after every iteration?
    
    seen_discard_clusters     = []
    seen_compression_clusters = []

    #step 13. Merge CS and DS clusters.
    for c_cluster in compression_list:
        c_cluster_centroid = c_cluster.centroid
        for d_cluster in discard_list:
            if c_cluster in seen_compression_clusters or d_cluster in seen_discard_clusters:
                continue
            # calculate maha_dist between the centroids.
            d_cluster_centroid = d_cluster.centroid
            maha_dist     = math.sqrt(sum([((c_cluster_centroid[i] - d_cluster_centroid[i])/c_cluster.standard_deviation[i])**2 for i in range(len(c_cluster_centroid))]))
            if maha_dist < 2 * math.sqrt(len(c_cluster_centroid)):
                new_points_list = c_cluster.points_list + d_cluster.points_list
                seen_discard_clusters.append(d_cluster)
                seen_compression_clusters.append(c_cluster)
                discard_list.append(Cluster(new_points_list))
    
    for cluster in seen_discard_clusters:
        discard_list.remove(cluster)

    for cluster in seen_compression_clusters:
        compression_list.remove(cluster)

    points_cluster_idx = []
    for idx, cluster_obj in enumerate(discard_list):
        points_list = cluster_obj.points_list
        for point in points_list:
            points_cluster_idx.append((int(point[0]), idx))
    
    for cluster_obj in compression_list:
        points_list = cluster_obj.points_list
        for point in points_list:
            points_cluster_idx.append((int(point[0]), -1))

    for point in retained_list:
        points_cluster_idx.append((int(point[0]), -1))

    
    with open(output_file, 'a+') as f:
        f.write("\nThe clustering results:\n")
        for point_cluster in sorted(points_cluster_idx, key = lambda x : x[0]):
            s = ','.join([str(point_cluster[0]), str(point_cluster[1])])
            f.write(s + "\n")


if __name__ ==  '__main__':   
    main()