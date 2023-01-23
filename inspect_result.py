'''
python3 inspect_result.py logs_dir (with \)
e.g. python3 inspect_result.py logs/2022.04.16-15.36.49/ ss

python3 inspect_result.py logs/ss/ ss
python3 inspect_result.py logs/hs/ hs
python3 inspect_result.py logs/sh/ sh
'''

import argparse, os, json
import copy
from collections import defaultdict
import re

interface = "ss"
table_prefix, hs_table_prefix, difft_prefix, eh_prefix, wr_prefix = "t_" , "t_r_", "difft_", "eh_", "wr_"
# prefixed with: log_dir
expected_table_file, original_table_file = "t_expected", "t_original.json"

exception_line_patterns = ["error:", "Exception:", "InsertIntoStatement", "mismatched input", "safely cast", 
"unresolvedalias", "Cannot", "Error parsing", "not supported", "Can only", "does not match", "Table not found", "illegal character"]
interface_split_symbol_dict = {"sql": "\t", "df": "|", "hql": "\t"}
interface_offset_dict = {"sql": None, "df": -1, "hql": None}
no_output_place_holder = "No output"
write_interfaces_dict = {"hs": ["hql"], "ss": ["sql", "df"], "sh": ["sql", "df"]}
format_types = ["avro", "orc", "parquet"]


convert_map = {"NULL": "null", "\t": " ", "\\\\": "\\"}

equivalence_classes = [
    {"NULL", "null"},
    {"{-2147483648:{12831273.24:3.141592653589793E-305}}", "{-2147483648 -> {12831273.24 -> 3.141592653589793E-305}}", "{-2147483648:{\"12831273.24\":3.141592653589793E-305}}", "{-2147483648:{12831273.24 3.141592653589793E-305"},
    {"{12831273.24:3.141592653589793E-305}", "{12831273.24 -> 3.141592653589793E-305}", "{\"12831273.24\":3.141592653589793E-305}", "{12831273.24 3.141592653589793E-305"},
    {"[[3.141592653589793E-305], [3.141592653589793E-305, 3.142E-320]]", "[[3.141592653589793E-305],[3.141592653589793E-305,3.142E-320]]"},
    {"^fo\\\\o$", "^fo\\o$"},
    {"8.88888888888889E9", "8888888888.8888900000"}
]

newlines_to_search = 30
log_start_line = 1 # sql 880 / df 1382


def _decode(o):
    if isinstance(o, str):
        try:
            return int(o)
        except ValueError:
            return o
    elif isinstance(o, dict):
        return {k: _decode(v) for k, v in o.items()}
    elif isinstance(o, list):
        return [_decode(v) for v in o]
    else:
        return o


def get_write_values(write_table_files):
    '''
    "0": {
        "sql":
            {"avro": "null"}
    }
    '''
    write_val_dict = defaultdict(lambda: defaultdict(dict))
    initial_dict = defaultdict(lambda: defaultdict(dict))

    for write_interface in write_interfaces_dict[interface]:
        initial_dict[write_interface] = {}
        for format_type in format_types:
            initial_dict[write_interface][format_type] = no_output_place_holder
    for row in original_dict:
        write_val_dict[row] = copy.deepcopy(initial_dict)
    for table_file in write_table_files:
        _, _, write_interface, format_type = table_file.split("_")
        split_symbol = interface_split_symbol_dict[write_interface]
        with open(log_dir + table_file, "r") as infile:
            content = infile.read().split('\n')
            for line in content:
                if len(line) == 0:
                    break
                row = line.split(split_symbol)[0].strip()
                val = line[line.index(split_symbol)+1: interface_offset_dict[write_interface]].strip()
                for key in convert_map:
                    val = val.replace(key, convert_map[key])
                write_val_dict[row][write_interface][format_type] = val
    # print(json.dumps(write_val_dict, indent=4, separators=(',', ':')))
    return write_val_dict


def parse_table_filename(table_file):
    _, _, write_interface, _, read_interface, format_type = table_file.split("_")
    return write_interface, read_interface, format_type


def get_table_log_files(table_file):
    w_log_filename = table_file.replace("t_", "log_").replace("r_sql_", "").replace("r_df_", "").replace("r_hql_", "")
    r_log_filename = table_file.replace("t_", "log_")
    return w_log_filename, r_log_filename


def analyze_input_behaviour_across_interfaces(test_inputs):
    input_behaviour_across_interfaces = dict()
    row_initial_dict = dict()
    write_val_dict = get_write_values(write_table_files)
    for table_file in read_table_files:
        write_interface, read_interface, format_type = parse_table_filename(table_file)
        row_dict = {
            "write_interface": write_interface,
            "read_interface": read_interface,
            "format_type": format_type,
            "read_value": no_output_place_holder,
            "write_value": no_output_place_holder
            }
        row_initial_dict[table_file] = row_dict
    # insert write values
    for row in test_inputs:
        input_behaviour_across_interfaces[row] = copy.deepcopy(row_initial_dict)
        for table_file in read_table_files:
            write_interface, read_interface, format_type = parse_table_filename(table_file)
            input_behaviour_across_interfaces[row][table_file]["write_value"] = write_val_dict[row][write_interface][format_type]
    for table_file in read_table_files:
        write_interface, read_interface, format_type = parse_table_filename(table_file)
        split_symbol = interface_split_symbol_dict[read_interface]
        with open(log_dir + table_file, "r") as infile:
            content = infile.read().split('\n')
            for line in content:
                if len(line) == 0:
                    break
                row = line.split(split_symbol)[0].strip()
                if row not in test_inputs:
                    # This is not a valid test input, currently we perform WR for ALL inputs
                    # in the spark_hive_oneway.sh & hive_spark_oneway.sh, this has to be changed
                    # there. Till then, we should make sure our oracle is smart enough to filter out
                    # unwanted inputs.
                    continue
                val = line[line.index(split_symbol)+1: interface_offset_dict[read_interface]].strip()
                for key in convert_map:
                    val = val.replace(key, convert_map[key])
                if row not in input_behaviour_across_interfaces:
                    input_behaviour_across_interfaces[row] = copy.deepcopy(row_initial_dict)
                input_behaviour_across_interfaces[row][table_file]["read_value"] = val
    # check exceptions
    for row, row_dict in input_behaviour_across_interfaces.items():
        for table_file in row_dict:
            w_log_filename, r_log_filename = get_table_log_files(table_file)            
            file_type_dict = {w_log_filename: "write", r_log_filename: "read"}
            for logfile in [w_log_filename, r_log_filename]:
                target_value = f"{file_type_dict[logfile]}_value"
                if no_output_place_holder in row_dict[table_file]["read_value"]:
                    previous_line_patterns = [f"insert into ws{row} ", f"val rdd{row} ", f"df{row}.show", f"select * from ws{row};"]
                    next_row_patterns = [f"insert into ws{int(row)+1} ", f"val rdd{int(row)+1} ", f"df{int(row)+1}.show", f"select * from ws{int(row)+1};"]
                    found = False
                    with open(log_dir + logfile, "r") as infile:
                        lines = infile.readlines()
                        stopFlag = False
                        for i in range(log_start_line, len(lines)):
                            if any(pattern in lines[i] for pattern in previous_line_patterns):
                                j = i + 1
                                while j < len(lines) and j < i + newlines_to_search and not found and not any(pattern in lines[j] for pattern in next_row_patterns):
                                    if any (exception in lines[j] for exception in exception_line_patterns):
                                        found = True
                                        row_dict[table_file][target_value] += ", find exception: {}".format(lines[j].strip())
                                        row_dict[table_file]["log_location"] = "({}) {} (line {})".format(file_type_dict[logfile], logfile, j+1)
                                        break
                                    j += 1
                            if stopFlag:
                                break
    with open(log_dir + interface + "_ungrouped_results.json", "w") as outfile:
        json.dump(input_behaviour_across_interfaces, outfile, indent=4, separators=(',', ':'))
    return input_behaviour_across_interfaces


def perform_error_handling_testing(input_behaviour_dict):
    all_eh = defaultdict(dict)
    failed_eh = defaultdict(dict)
    for _input, input_behaviour in input_behaviour_dict.items():
        if not original_dict[_input]['valid']:
            for ifc_format_combo, _input_behaviour in input_behaviour.items():
                test_result = copy.deepcopy(_input_behaviour)
                if "No output" not in _input_behaviour["read_value"]:
                    test_result["pass"] = False
                    failed_eh[_input][ifc_format_combo] = test_result
                else:
                    test_result["pass"] = True
                all_eh[_input][ifc_format_combo] = test_result

    # Dumping all EH tests to <ifc>_eh_all.json
    with open(log_dir + interface + "_eh_all.json", "w") as outfile:
        json.dump(all_eh, outfile, indent=4, separators=(',', ':'))

    # Dumping the failed EH tests to <ifc>_eh_failed.json
    with open(log_dir + interface + "_eh_failed.json", "w") as outfile:
        json.dump(failed_eh, outfile, indent=4, separators=(',', ':'))


def perform_differential_testing(input_behaviour_dict):
    difft_row_compact = dict()
    failed_difft_tests = dict()
    for row, row_dict in input_behaviour_dict.items():
        tests = defaultdict(list)
        for table_file in row_dict:
            table_file_dict = row_dict[table_file]
            actual_val = table_file_dict["read_value"]
            # canonicalize the data values
            test_metadata = {
                "write_interface": table_file_dict["write_interface"],
                "read_interface": table_file_dict["read_interface"],
                "format_type": table_file_dict["format_type"],
            }
            if "log_location" in table_file_dict:
                test_metadata["log_location"] = table_file_dict["log_location"]

            tests[actual_val].append(test_metadata)

        if len(tests) == 1:
            has_pass = True
        else:
            no_output_count = equivalent_class_count = regular_count = 0
            for val in tests:
                if "No output" in val:
                    no_output_count = 1
                elif any(val in equivalence_class for equivalence_class in equivalence_classes):
                    equivalent_class_count = 1
                else:
                    regular_count += 1
            if no_output_count + equivalent_class_count + regular_count > 1:
                has_pass = False
            else:
                has_pass = True

        test_result = {
            "original_value": original_dict[row]["value"],
            "type": original_dict[row]["type"],
            "valid": original_dict[row]["valid"],
            "output": tests,
            "pass": has_pass
        }
        difft_row_compact[row] = test_result
        if not has_pass:
            failed_difft_tests[row] = test_result

    # Dumping all the DiffT tests to <ifc>_difft_row_compact.json
    difft_row_compact_json = json.dumps(difft_row_compact, indent=4, separators=(',', ':'))
    # TODO replace with regex
    difft_row_compact_json = difft_row_compact_json.replace('{\n                    "write_interface":"', '{"write_interface":"').replace('\n                    "read_interface":"','  "read_interface":"').replace('\n                    "format_type":"', ' "format_type":"').replace('\n                    "log_location":"', '  "log_location":"').replace('\n                }', '}')
    with open(log_dir + interface + "_difft_all.json", "w") as outfile:
        outfile.write(difft_row_compact_json)

    # Dumping the failed DiffT tests to <ifc>_difft_failed.json
    with open(log_dir + interface + "_difft_failed.json", "w") as outfile:
        json.dump(failed_difft_tests, outfile, indent=4, separators=(',', ':'))

    return difft_row_compact


def perform_write_read_testing(input_behaviour_dict):
    all_wr = defaultdict(dict)
    failed_wr = defaultdict(dict)
    for _input, input_behaviour in input_behaviour_dict.items():
        if original_dict[str(_input)]['valid'] == True:
            for ifc_format_combo, _input_behaviour in input_behaviour.items():
                test_result = copy.deepcopy(_input_behaviour)
                read_value = _input_behaviour["read_value"]
                write_value = _input_behaviour["write_value"]
                if read_value != write_value and not (no_output_place_holder in read_value and no_output_place_holder in write_value) \
                    and not any(read_value in equivalence_class and write_value in equivalence_class for equivalence_class in equivalence_classes):
                    test_result["pass"] = False
                    failed_wr[str(_input)][ifc_format_combo] = test_result
                else:
                    test_result["pass"] = True
                all_wr[str(_input)][ifc_format_combo] = test_result

    # Dumping all wr tests to <ifc>_wr_all.json
    with open(log_dir + interface + "_wr_all.json", "w") as outfile:
        json.dump(all_wr, outfile, indent=4, separators=(',', ':'))

    # Dumping the failed wr tests to <ifc>_wr_failed.json
    with open(log_dir + interface + "_wr_failed.json", "w") as outfile:
        json.dump(failed_wr, outfile, indent=4, separators=(',', ':'))


def get_expected_vals(log_dir):
    expected_dict = {}
    with open(log_dir + expected_table_file, "r") as infile:
        content = infile.read().split('\n')
        for line in content:
            if len(line) > 0:
                row, val = line.split('\t')
                expected_dict[row] = val.strip()
    return expected_dict 


def get_original_vals(log_dir):
    with open(log_dir + original_table_file, "r") as infile:
        original_dict = json.load(infile, object_hook=_decode)
    return original_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('log_dir', type=str)
    parser.add_argument('interface', type=str)
    parser.add_argument('--dry_run', action='store_true')
    args = parser.parse_args()
    log_dir = args.log_dir
    interface = args.interface
    log_files = os.listdir(log_dir)
    write_table_files = list(filter(lambda x: (x.startswith(table_prefix) and "_w_" in x and "_r_" not in x), log_files))
    read_table_files = list(filter(lambda x: (x.startswith(table_prefix) and "_r_" in x), log_files))

    original_dict = get_original_vals(args.log_dir)
    # expected_dict = get_expected_vals(args.log_dir)

    test_inputs = None
    if interface in ['sh', 'hs']:
        #  We only consider valid inputs for Spark-Hive & Hive-Spark oneway testing, because it is a test of
        # interoperability. Injecting invalid values only makes sense if one is testing an interface in isolation.
        test_inputs = {input_idx: input_metadata for input_idx, input_metadata
                       in original_dict.items() if input_metadata.get('valid', True)}
    else:
        test_inputs = original_dict

    if args.dry_run:
        with open(log_dir + interface + "_ungrouped_results.json", "r") as infile:
            input_behaviour_dict = json.load(infile)
    else:
        input_behaviour_dict = analyze_input_behaviour_across_interfaces(test_inputs)

    perform_differential_testing(input_behaviour_dict)
    perform_write_read_testing(input_behaviour_dict)
    if interface == 'ss':
        perform_error_handling_testing(input_behaviour_dict)
