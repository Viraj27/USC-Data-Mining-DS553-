from time import time
from blackbox import BlackBox
import sys
import random
import binascii
import csv


FILTER_BIT_ARRAY = [0] * 69997
_PRIME_NUMBER    = 999999991
len_array_bits   = len(FILTER_BIT_ARRAY)
hash_func_num    = 5
previously_seen_user_set = set()
false_positive_data = []

def create_hash_functions():

    def generate_hash_function():
        a, b = random.randint(2, sys.maxsize-1), random.randint(2, sys.maxsize-1)
        return lambda x : ((a*x+b)%_PRIME_NUMBER)%len_array_bits
    hash_functions_list = []
    for _ in range(hash_func_num):
        hash_func = generate_hash_function()
        hash_functions_list.append(hash_func)
    return hash_functions_list

hash_functions_list = create_hash_functions()

def myhashs(user):
    user = int(binascii.hexlify(user.encode('utf8')),16)
    hash_list = []
    for hash_func in hash_functions_list:
        hash_list.append(hash_func(user))
    return hash_list

def bloom_filter(user, true_negative_users, false_positive_users):
    hash_list = myhashs(user)
    user_seen_flag = True
    for idx in hash_list:
        if FILTER_BIT_ARRAY[idx] == 0:
            user_seen_flag = False
            break
    if user not in previously_seen_user_set:
        # false positive case
        if user_seen_flag:
            false_positive_users.append(user)
        else:
            true_negative_users.append(user)
    previously_seen_user_set.add(user)
    for idx in hash_list:
        FILTER_BIT_ARRAY[idx] = 1

def main():
    start_time = time()
    input_file       = sys.argv[1]
    stream_size      = int(sys.argv[2])
    num_asks         = int(sys.argv[3])
    output_file      = sys.argv[4]
    bx = BlackBox()
    for i in range(num_asks):
        stream_users   = bx.ask(input_file, stream_size)
        true_negative_users  = []
        false_positive_users = []
        for user in stream_users:
            bloom_filter(user, true_negative_users, false_positive_users)
        false_positive_rate = len(false_positive_users) / (len(true_negative_users) + len(false_positive_users))
        false_positive_data.append((i, false_positive_rate))
        
    with open(output_file,'w+') as f:
        writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
        writer.writerow(['Time', 'FPR'])
        for data in false_positive_data:
            writer.writerow([data[0], data[1]])

    print('Duration : ', time() - start_time)



if __name__ == "__main__":
    main()