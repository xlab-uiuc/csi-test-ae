import argparse
import glob
import os
import re


def get_int(s):
    return re.sub("[^0-9]", "", s)


def get_tags(filename):
    m = set()
    with open(filename, 'r') as rf:
        for line in rf:
            # elements = re.split('\||\t', line.replace('\n', ''))
            elements = line.split()
            elements = [x for x in elements if x != ""]
            if elements[0][0].isdigit():
                idx = int(elements[0])
                m.add(idx)
    return m


parser = argparse.ArgumentParser()
parser.add_argument('log_dir', type=str)
args = parser.parse_args()

wr_files = glob.glob(os.path.join(args.log_dir, "wr*"))
eh_files = glob.glob(os.path.join(args.log_dir, "eh*"))
difft_files = glob.glob(os.path.join(args.log_dir, "difft*"))

wr_fail = 0
for txt in wr_files:
    f = open(txt, 'r')
    for line in f:
        if "write read total" in line:
            wr_fail += int(get_int(line))

eh_fail = 0
for txt in eh_files:
    f = open(txt, 'r')
    for line in f:
        if "error handling total" in line:
            eh_fail += int(get_int(line))

difft_valid_fail = 0
difft_invalid_fail = 0

valid_ids = get_tags(os.path.join(args.log_dir, "t_expected"))
valid_counted = set()
invalid_counted = set()

for txt in difft_files:
    difft_tags = get_tags(txt)
    for t in difft_tags:
        if t in valid_ids:
            valid_counted.add(t)
        else:
            invalid_counted.add(t)

    # for line in f:
    #     if "write read total" in line:
    #         difft_valid_fail += int(get_int(line))
    #     if "error handling total" in line:
    #         difft_invalid_fail += int(get_int(line))

print("wr fails:", wr_fail)
print("eh fails:", eh_fail)
print("difft fails:", len(valid_counted), len(invalid_counted))