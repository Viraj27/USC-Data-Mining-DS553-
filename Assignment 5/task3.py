from time import time
from blackbox import BlackBox
import sys
import random
import csv


num_asks         = int(sys.argv[3])
stream_size      = int(sys.argv[2])
random.seed(553)
fixed_size_sampling_list = [''] * 100
user_sequence_number = 0
input_file       = sys.argv[1]
output_file      = sys.argv[4]
bx = BlackBox()
i = 0
start_time = time()

def write_data_to_csv():
    with open(output_file,'a') as f:
        writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
        writer.writerow([user_sequence_number, fixed_size_sampling_list[0], fixed_size_sampling_list[20], fixed_size_sampling_list[40], fixed_size_sampling_list[60], fixed_size_sampling_list[80]])

# Create the output csv file and write the column names to it.
with open(output_file, 'a') as f:
    writer = csv.writer(f, delimiter=',',quoting=csv.QUOTE_NONE)
    writer.writerow(["seqnum", "0_id", "20_id", "40_id", "60_id", "80_id"])

for _ in range(num_asks):
    stream_users   = bx.ask(input_file, stream_size)
    for user in stream_users:
        user_sequence_number += 1
        if user_sequence_number > len(fixed_size_sampling_list):
            # generate random probability and then compare with calculated s/n
            if (len(stream_users) / user_sequence_number ) > random.random():
                fixed_size_sampling_list[random.randint(0, len(stream_users) - 1)] = user
        else:
            fixed_size_sampling_list[i] = user
            i += 1
    write_data_to_csv()

print('Duration : ', time() - start_time)