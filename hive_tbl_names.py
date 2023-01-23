import argparse

def get_tbl_gets(input_file, spark_wh, format):
    with open(input_file, 'r') as rf:
        lines = rf.readlines()
        creates = [line for line in lines if line.lower().startswith('create')]
    names = [create.split()[2].split('(')[0] for create in creates]
    gets = ['hdfs dfs -getmerge /user/hive/warehouse/' + name
        + ' ' + spark_wh + '/' + name + '.' + format + '\n' for name in names]
    return gets

def write_cmds(cmds, output_file):
    with open(output_file, 'w') as wf:
        for c in cmds:
            print(c, end='')
            wf.write(c)

parser = argparse.ArgumentParser()
parser.add_argument('input_file', type=str)
parser.add_argument('output_file', type=str)
parser.add_argument('spark_wh', type=str)
parser.add_argument('format', type=str)
args = parser.parse_args()

gets = get_tbl_gets(args.input_file, args.spark_wh, args.format)
write_cmds(gets, args.output_file)