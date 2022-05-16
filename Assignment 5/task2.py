from time import time
from blackbox import BlackBox
import sys
import random
import math
import binascii
from statistics import median
import csv

_PRIME_NUMBER    = 999999991
_MODULO_NUM      = 1000
_HASH_FUNC_NUM   = 100
result           = []

def create_hash_functions():

    def generate_hash_function():
        a, b = random.randint(2, 10000000000), random.randint(2, 10000000000)
        return lambda x : ((a*x+b)%_PRIME_NUMBER)%_MODULO_NUM
    hash_functions_list = []
    for _ in range(_HASH_FUNC_NUM):
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

def find_trailing_zeros(hash_value):
    bin_repr =  str(bin(hash_value)[2:])
    return len(bin_repr) - len(bin_repr.rstrip('0'))

def main():
    num_asks         = int(sys.argv[3])
    stream_size      = int(sys.argv[2])
    input_file       = sys.argv[1]
    output_file      = sys.argv[4]
    start_time = time()
    bx = BlackBox()
    for i in range(num_asks):
        max_trail_zeros_per_user = [0] * 100
        stream_users   = bx.ask(input_file, stream_size)
        for user in stream_users:
            hash_list = myhashs(user)
            for j in range(len(hash_list)):
                hash_list[j] = find_trailing_zeros(hash_list[j])
            # hash_list now contains binary representation of hash values.
            # Now, find trailing zeros for each and update the maximum in max_trailing_zeros list.
                max_trail_zeros_per_user[j] = max(max_trail_zeros_per_user[j], hash_list[j])
        
        fm_number_list = []
        for k in max_trail_zeros_per_user:
            fm_number_list.append(math.pow(2,k))
        fm_average_number_list = []
        for z in range(0, _HASH_FUNC_NUM, 10):
            fm_average_number_list.append(sum(fm_number_list[z:z+10])/10)

        fm_number = median(fm_average_number_list)
        original_number = len(set(stream_users))
        result.append((i, original_number, int(fm_number)))

    
    with open(output_file,'w+') as f:
        writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
        writer.writerow(['Time', 'Ground Truth', 'Estimation'])
        for data in result:
            writer.writerow([data[0], data[1], data[2]])

    print('Duration : ', time() - start_time)
            

if __name__ == "__main__":
    main()
