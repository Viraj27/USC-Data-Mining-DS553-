from pyspark     import SparkContext, SparkConf
from helper      import User_Node
from time import time
from collections import defaultdict
import sys
import operator, copy

# class User_Node():
#     def __init__(self, user_id, parents_set, childrens_set, level_from_root, shortest_path_from_root):
#         self.user_id = user_id or ''
#         self.parents_set = parents_set or set()
#         self.childrens_set = childrens_set or set()
#         self.level_from_root = level_from_root or 0
#         self.shortest_path_from_root = shortest_path_from_root or 0

def find_neighbors_set(row, user_bid_map, filter_threshold):
    neighbors_set = set()
    user_id = row[0]
    business_set = row[1]
    
    for potential_neighbor, their_business_set in user_bid_map.items():
        ########## CHANGE THIS TO THRESHOLD. ###############
        if user_id != potential_neighbor and len(business_set.intersection(their_business_set)) >= filter_threshold:
            neighbors_set.add(potential_neighbor)
    
    return (user_id, neighbors_set)

def generate_bfs_tree(row, neighbors_set_data):
    user_id = row[0]
    neighbors_set = row[1]
    
    # create User_Node for user_id which will be root for this row. 
    #For our case, shortest path from root to root = 0 and root level = 0.
    
    user_node = User_Node(user_id, set(), set(), 0, 1, 0)
    
    # BFS queue - just a normal list.
    queue = []
    # visited nodes - tree representation. 
    visited = dict()
    
    queue.append(user_id)
    visited[user_id] = user_node
    level = 1
    #start BFS
    while queue:
        level_size = len(queue)
        for _ in range(level_size):
            current_user_id   = queue.pop(0)
            current_user_node = visited[current_user_id]
            for neighbor in neighbors_set_data[current_user_id]:
                if neighbor in visited:
                    neighbor_node = visited[neighbor]
                    if neighbor_node in current_user_node.parents_set or current_user_node.level_from_root == neighbor_node.level_from_root:
                        continue
                    neighbor_node.parents_set.add(current_user_node)
                    current_user_node.childrens_set.add(neighbor_node)
                    
                else:
                    neighbor_node = User_Node(neighbor, set(), set(), level, 1, 0)
                    # creating parent-child linkage.
                    neighbor_node.parents_set.add(current_user_node)
                    current_user_node.childrens_set.add(neighbor_node)
                    # add this node to queue and visited
                    queue.append(neighbor)
                    visited[neighbor] = neighbor_node
                    
        level += 1
    
    return (user_id, visited)

def generate_shortest_paths_count(row):
    user_id = row[0]
    bfs_tree = row[1]
    levels_dict = defaultdict(set)
    visited     = set()
    # root node - number of shortest paths = 1. 
    bfs_tree[user_id].shortest_paths_from_root = 1
    #Another BFS through the tree to add the nodes on each level to respective level key in dict
    queue = []
    queue.append(user_id)
    visited.add(user_id)
    while queue:
        current_user_id = queue.pop(0)
        current_user_node = bfs_tree[current_user_id]
        current_node_level = current_user_node.level_from_root
        levels_dict[current_node_level].add(current_user_node)
        if len(current_user_node.parents_set) == 0:
            current_user_node.shortest_paths_from_root = 1
        # By design, root node shortest paths = 1.
        num_shortest_paths = 0
        if current_user_node.parents_set:
            for parent in current_user_node.parents_set:
                num_shortest_paths += parent.shortest_paths_from_root
            current_user_node.shortest_paths_from_root = num_shortest_paths
        
        for children in current_user_node.childrens_set:
            if children.user_id not in visited:
                queue.append(children.user_id)
                visited.add(children.user_id)
    
    return (user_id, dict(sorted(levels_dict.items(), reverse=True)))

def is_leaf_node(node):
    return len(node.childrens_set) == 0

def generate_credit_allocation(row):
    edge_credit_map = dict()
    user_id         = row[0]
    levels_dict     = row[1]

    for _, nodes in levels_dict.items():
        for node in nodes:
            # if child node 
            if is_leaf_node(node):
                node.credit = 1.0
            else:
                #calculate edge sum first
                sum_edges = 0.0
                for child_node in node.childrens_set:
                    # edge will be the edge between parent and child
                    edge = tuple(sorted([child_node.user_id, node.user_id]))
                    sum_edges += edge_credit_map[edge]
                node.credit = 1.0 + sum_edges
            
            if len(node.parents_set) > 0:
                # calculate fraction of weight assignment for edge.
                total_credits = sum([p.shortest_paths_from_root for p in node.parents_set])
                for parent_node in node.parents_set:
                    edge = tuple(sorted([parent_node.user_id, node.user_id]))
                    edge_credit_map[edge] = (parent_node.shortest_paths_from_root / total_credits) * node.credit
    
    return (user_id, edge_credit_map)
                    
def calculate_betweenness(credit_allocated_map):
    
    edge_betweenness_map = defaultdict(float)
    for _, edge_credit_map in credit_allocated_map.items(): 
        for edge, credit in edge_credit_map.items():
            edge_betweenness_map[edge] += credit

    edge_betweenness_list = [(edge, round(credit/2.0, 5)) for edge, credit in edge_betweenness_map.items()]
    edge_betweenness_list.sort(key=operator.itemgetter(0))
    return sorted(edge_betweenness_list, key = operator.itemgetter(1), reverse = True)

    #return edge_betweenness_list

                    
conf = SparkConf().setAppName('DM_assign4_task21').setMaster('local[*]')
sc   = SparkContext.getOrCreate(conf = conf)

sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
sc.setLogLevel('ERROR')

filter_threshold, input_file, betweenness_file, community_output_file = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
#filter_threshold, input_file, community_output_file = int(sys.argv[1]), sys.argv[2], sys.argv[3]
input_data_rdd          = sc.textFile(input_file)\
                            .map(lambda row : row.split(','))
        
csv_header_data      = input_data_rdd.first()

input_data_rdd       = input_data_rdd.filter(lambda row : row != csv_header_data).map(lambda x : (x[0], {x[1]})).reduceByKey(lambda a, b : a | b)


user_bid_map         = input_data_rdd.collectAsMap()

# create the graph

neighbors_set_rdd               = input_data_rdd.map(lambda row : find_neighbors_set(row, user_bid_map, filter_threshold)).filter(lambda row : len(row[1]) > 0)

neighbors_set_data              = neighbors_set_rdd.collectAsMap()

# graph creation done.

######## calculating betweenness.######## 

# for each node, have to create BFS tree construction.
def beetweenness_finder(neighbors_set_rdd, neighbors_set_data):
    bfs_tree_rdd    = neighbors_set_rdd.map(lambda row : generate_bfs_tree(row, neighbors_set_data))

    # BFS tree complete.

    # Assign shortest path label for each node in the tree.

    shortest_path_rdd = bfs_tree_rdd.map(lambda row : generate_shortest_paths_count(row))

    credit_allocated_map = shortest_path_rdd.map(lambda row : generate_credit_allocation(row)).collectAsMap()

    return calculate_betweenness(credit_allocated_map)

######## calculating betweenness.######## 

edge_betweenness_list = beetweenness_finder(neighbors_set_rdd, neighbors_set_data)
last_idx = len(edge_betweenness_list) - 1

with open(betweenness_file, 'w') as f:
    for idx, edge_betweenness in enumerate(edge_betweenness_list):
        s = "('" + str(edge_betweenness[0][0]) + "', '" + str(edge_betweenness[0][1]) + "'),"+str(edge_betweenness[1])
        f.write(s)
        if idx != last_idx:
            f.write('\n')


############ find communities ###############

# find_communities - BFS through the graph. Find connected components and add them to list of communities.
    
# # calculate Q score using formula given

# # detect_suitable_communities - remove heaviest edge - find communities (BFS) - calculate Q - append to Qlist - repeat. 

def evaluate_Q_score(A, m, communities):
    
    q_score_list = []
    for community in communities:
        q_score = 0
        for node_1 in community:
            for node_2 in community:
                matrix_a_edge_value = 0
                if node_2 in A[node_1]:
                    matrix_a_edge_value = 1
                expected_value = (len(A[node_1]) * len(A[node_2]) / (2.0 * m))
                q_score += (matrix_a_edge_value - expected_value) / (2.0 * m)
        q_score_list.append(q_score)
    
    return sum(q_score_list)

def detect_community_in_graph(node, graph, connected_components):
    visited_nodes = set()
    visited_nodes.add(node)
    queue = []
    queue.append(node)
    while queue:
        level_size = len(queue)
        for _ in range(level_size):
            current_node = queue.pop(0)
            for neighbor in graph[current_node]:
                if neighbor in visited_nodes:
                    continue
                visited_nodes.add(neighbor)
                connected_components.add(neighbor)
                queue.append(neighbor)
    return visited_nodes

def communities_in_graph(graph):
    graph_to_traverse = copy.deepcopy(graph)
    connected_components = set()
    communities = list()
    
    for node, _ in graph_to_traverse.items():
        if node in connected_components:
            continue
        connected_components.add(node)
        community = detect_community_in_graph(node, graph_to_traverse, connected_components)
        communities.append(list(community))
    return communities

def detect_suitable_communities(iteration_length, m, heaviest_edge, graph, A, qs):
    communities = []
    most_suitable_communities = None
    max_q_score = float('-inf')
    for x in range(iteration_length):
        graph[heaviest_edge[0][0]].discard(heaviest_edge[0][1])
        graph[heaviest_edge[0][1]].discard(heaviest_edge[0][0])
        communities = communities_in_graph(graph)
        q_score = evaluate_Q_score(A,m,communities)
        if q_score > max_q_score:
            max_q_score = q_score
            most_suitable_communities = communities
        qs.append(evaluate_Q_score(A, m, communities))
        if x != m- 1:
            graph_rdd = sc.parallelize(list(graph.items()))
            heaviest_edge = beetweenness_finder(graph_rdd, graph)[0]
    return most_suitable_communities

m = len(edge_betweenness_list)
graph = copy.deepcopy(neighbors_set_data)
A = copy.deepcopy(graph)
qs = list()
heaviest_edge = edge_betweenness_list[0]
most_suitable_communities = detect_suitable_communities(m, m, heaviest_edge, graph, A, qs)
for community in most_suitable_communities:
    community.sort()
most_suitable_communities = sorted(most_suitable_communities, key = lambda x : (len(x), x[0]))

last_idx = len(most_suitable_communities) - 1
with open(community_output_file, 'w') as f:
    for idx, community in enumerate(most_suitable_communities):
        s = ''
        for user in community:
            s += "'" + user + "'" + ',' + ' '
        s = s.rstrip(', ')
        f.write(s)
        if idx != last_idx:
            f.write('\n')