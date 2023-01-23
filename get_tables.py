import argparse
import re

parser = argparse.ArgumentParser()
parser.add_argument('input_file', type=str)
parser.add_argument('output_file', type=str)
parser.add_argument('--rt', action='store_true')
parser.add_argument('system', type=str)
args = parser.parse_args()
args.system += ">"

with open(args.input_file, 'r') as rf:
    with open(args.output_file, 'w') as wf:
        table = False
        for line in rf:
            if "select " in line or ".show(false)" in line:
                if ("df" in line or "select (" in line) and args.rt:
                    table = True
                elif ("select *" in line or ".show(false)" in line) and not args.rt:
                    table = True
            elif args.system in line:
                table = False
            if args.system not in line and table:
                if args.system in ["spark-sql>", "hive>"]:
                    if line[0].isdigit() and line.split("\t")[0].isdigit():
                        wf.write(line)
                    elif line[0] == '{':
                        w_line = '\t'.join([x for x in re.split('":|,"', line) if "col" not in x])
                        wf.write(w_line.replace("}", "").replace('"', ""))
                if args.system == "scala>":
                    if line[0] == "|" and line[1] != "c":
                        w_line = '\t'.join([x for x in line.split(' ') if x != ''])
                        wf.write(w_line[1:])
