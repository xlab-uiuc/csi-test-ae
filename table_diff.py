# because we cannot run multiple spark instances at once
# we can only do string comparison
import argparse
import re

equivalence_classes = [
    {"NULL", "null"},
    {'{"12831273.24":3.141592653589793E-305}', '{12831273.24->3.141592653589793E-305}',
     '{12831273.243.141592653589793E-305'},
    {'{-2147483648->{12831273.24->3.141592653589793E-305}}', '{-2147483648:{12831273.243.141592653589793E-305',
     '{-2147483648:{"12831273.24":3.141592653589793E-305}}'}

]

wr_fp = [
    (117, "spark"),
    (119, "25"),
    (120, "1.1"),
    (128, "1"),
    (130, "1.1")
]


def rm_whitespace(s):
    return s.strip().replace("\t", "").replace(" ", "")


def by_tag(filename):
    m = {}
    with open(filename, 'r') as rf:
        for line in rf:
            elements = re.split('\||\t', line.replace('\n', ''))
            elements = [x for x in elements if x != ""]
            idx = int(elements[0])
            m[idx] = ''.join(elements[1:])
    return m


def get_diff(m1, m2, first):
    stats = {"err": 0, "wr": 0}
    wr = []
    err = []
    for (k, v1) in m1.items():
        if k in m2.keys():
            if first == 0:
                m1_clean = rm_whitespace(m1[k])
                m2_clean = rm_whitespace(m2[k])
                if m1_clean != m2_clean:
                    in_same_equiv = False
                    for c in equivalence_classes:
                        if m1_clean in c and m2_clean in c:
                            in_same_equiv = True
                    if not in_same_equiv:
                        wr.append('{0} {1} || {2}'.format(k, m1[k], first))
                        wr.append('{0} {1} || {2}'.format(k, m2[k], 1-first))
                        stats["wr"] += 1
        else:
            if first == 0:
                if (k, m1[k]) not in wr_fp:
                    wr.append('{0} {1} || {2}'.format(k, m1[k], first))
                    stats["wr"] += 1
            else:
                err.append('{0} {1} || {2}'.format(k, m1[k], first))
                stats["err"] += 1
    return wr, err, stats


parser = argparse.ArgumentParser()
parser.add_argument('input_file1', type=str)
parser.add_argument('input_file2', type=str)
args = parser.parse_args()

tb1 = by_tag(args.input_file1)
tb2 = by_tag(args.input_file2)

wr0, err0, stats0 = get_diff(tb1, tb2, 0)
wr1, err1, stats1 = get_diff(tb2, tb1, 1)

print("-"*30)
print("write read")
for r in wr0+wr1:
    print(r)
print("-"*30)

print("-"*30)
print("error handling")
for r in err0+err1:
    print(r)
print("-"*30)
print("write read total: ", stats0["wr"] + stats1["wr"])
print("error handling total: ", stats0["err"] + stats1["err"])
