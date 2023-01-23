# used for spark->hive getting valid values

import argparse
import glob
import os
import re


def get_int(s):
    return re.sub("[^0-9]", "", s)


# TODO repeated w test failures method
def get_tags(filename):
    m = set()
    with open(filename, 'r') as rf:
        for line in rf:
            # elements = re.split('\||\t', line.replace('\n', ''))
            idx = get_tag(line)
            if idx is not None:
                m.add(idx)
    return m


def get_tag(line):
    elements = line.split()
    elements = [x for x in elements if x != ""]
    if elements[0][0].isdigit():
        return int(elements[0])
    return None


parser = argparse.ArgumentParser()
parser.add_argument('log_dir', type=str)
args = parser.parse_args()

difft_files = glob.glob(os.path.join(args.log_dir, "difft*"))

valid_ids = get_tags(os.path.join(args.log_dir, "t_expected"))
valid_counted = set()

for txt in difft_files:
    with open(os.path.join(args.log_dir, os.path.basename(txt)+"_filtered"), 'w') as wf:
        with open(txt, 'r') as unfiltered:
            for l in unfiltered:
                idx = get_tag(l)
                if idx in valid_ids:
                    wf.write(l)