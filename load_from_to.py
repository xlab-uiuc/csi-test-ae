import argparse
from os import listdir

def interfaces2translation(og_interface, final_interface):
    return {
        "sql": {"hql": sql2hqltypes},
        "hql": {"sql": hql2sqltypes}
    }[og_interface][final_interface]

sql2hqltypes = {
    "BYTE": "TINYINT",
    "SHORT": "SMALLINT",
    "INT": "INT",
    "LONG": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL",
    "STRING": "STRING",
    "VARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "BINARY": "BINARY",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "MAP<INT, MAP<STRING, DOUBLE>>": "MAP<INT, MAP<STRING, DOUBLE>>",
    "ARRAY<ARRAY<DOUBLE>>": "ARRAY<ARRAY<DOUBLE>>"
}

hql2sqltypes = {
    "TINYINT": "BYTE",
    "SMALLINT": "SHORT",
    "INT": "INT",
    "BIGINT": "LONG",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DECIMAL",
    "STRING": "STRING",
    "VARCHAR": "VARCHAR",
    "CHAR": "CHAR",
    "BINARY": "BINARY",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "MAP<INT, MAP<STRING, DOUBLE>>": "MAP<INT, MAP<STRING, DOUBLE>>",
    "ARRAY<ARRAY<DOUBLE>>": "ARRAY<ARRAY<DOUBLE>>"
}

def translate_creates(og_creates, translation):
    final_creates = []
    for ogc in og_creates:
        fc = ogc
        for ogtype, ftype in translation.entries():
            fc = fc.replace(ogtype.lower(), ftype)
        final_creates.append(fc)
    return final_creates

def read_cmds(input_file, og_interface, final_interface, og_wh, format):
    with open(input_file, 'r') as rf:
        lines = rf.readlines()
        og_creates = [line for line in lines if line.lower().startswith('create')]
        if final_interface in ['sql', 'hql']:
            drops = [line for line in lines if line.lower().startswith('drop')]
            creates = translate_creates(
                        og_creates,
                        interfaces2translation(og_interface, final_interface)
                    )
            selects = [line for line in lines if line.lower().startswith('select')]
            names = [create.split()[2].split('(')[0] for create in creates]
        else:
            selects = []
    loads = []
    for name in names:
        fn = og_wh + '/' + name + '.' + format
        if final_interface in ['sql', 'hql']:
            loads.append("load data local inpath '" + fn + "' into table " + name + ';\n')
        else:
            loads.append('val ' + name + ' = spark.read.format("' + format
                        + '").load("' + fn + '")\n')
            selects.extend([name + '.show(false)',
                            name + '.write.mode("overwrite").format("' + format + '").saveAsTable("' + name + '")',
                            'spark.sql("select * from ' + name + ';")'
                           ])
    return drops, creates, loads, selects

def write_cmds(cmds, output_file, mode):
    with open(output_file, mode) as wf:
        for c in cmds:
            print(c, end='')
            wf.write(c)


parser = argparse.ArgumentParser()
parser.add_argument('input_file', type=str)
parser.add_argument('output_file', type=str)
parser.add_argument('og_interface', type=str)
parser.add_argument('og_wh', type=str)
parser.add_argument('final_interface', type=str)
parser.add_argument('format', type=str)
args = parser.parse_args()

# len(drops) == len(creates) == len(loads) == len(selects)
drops, creates, loads, selects = read_cmds(
    args.input_file, args.og_interface, args.final_interface, args.og_wh, args.format)

mode = 'w'
if args.final_interface == 'scala':
    imports = ["import org.apache.spark.sql.{Row, SparkSession}\n",
               "import org.apache.spark.sql.types._\n",
               "import scala.util.{Try, Success, Failure}\n",
               "import scala.math.BigInt\n"]
    write_cmds(imports, args.output_file, 'w')
    mode = 'a'

write_cmds(drops, args.output_file, mode)
if args.final_interface in ['sql', 'hql']:
    write_cmds(creates, args.output_file, 'a')
write_cmds(loads, args.output_file, 'a')
write_cmds(selects, args.output_file, 'a')