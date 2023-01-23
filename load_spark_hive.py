import argparse
from os import listdir

def read_cmds(input_file, spark_wh):
    with open(input_file, 'r') as rf:
        lines = rf.readlines()
        drops = [line for line in lines if line.lower().startswith('drop')]
        creates = [line for line in lines if line.lower().startswith('create')]
        selects = [line for line in lines if line.lower().startswith('select')]
    names = [create.split()[2].split('(')[0] for create in creates]
    loads = []
    for name in names:
        dir = spark_wh + '/' + name + '/'
        fns = [fn for fn in listdir(dir) if fn.startswith('part')]
        loads.extend(["load data local inpath '" + dir + fn + "' into table " + name + ';\n'
                      for fn in fns])
    return drops, creates, loads, selects

def write_cmds(cmds, output_file, mode):
    with open(output_file, mode) as wf:
        for c in cmds:
            print(c, end='')
            wf.write(c)


parser = argparse.ArgumentParser()
parser.add_argument('input_file', type=str)
parser.add_argument('output_file', type=str)
parser.add_argument('spark_wh', type=str)
args = parser.parse_args()

# len(drops) == len(creates) == len(loads) == len(selects)
drops, creates, loads, selects = read_cmds(args.input_file, args.spark_wh)

write_cmds(drops, args.output_file, 'w')
write_cmds(creates, args.output_file, 'a')
write_cmds(loads, args.output_file, 'a')
write_cmds(selects, args.output_file, 'a')