### Individual files and logs

`value_gen.py`: generates SQL statements to execute. contains the value-picking logic and embeds those values into a sequence of CREATE TABLE/INSERT/SELECT. also writes the ground truth/expected values into a file for round trip test checking purposes.

`logs/…/gen{0,1}`: the resulting statements
`logs/…/rt`: the round trip test checking values

`python3 value_gen.py <format> logs/…/rt`

----

`translate_gen.py`: translates a sequence of SQL statements to be executed in a different setup. for now this consists of swapping the row format used
outputs the other `logs/…/gen1`

`python3 translate_gen.py <original_path> <translated_path> --format <format>`

`python3 translate_gen.py logs/…/gen0 logs/…/gen1 --format <format>`

`logs/…/log{0,1}`: the piped outputs from the execution of the SQL statements on each setup

----

`get_tables.py`: from the `logs/…/log{0,1}` output all generated tables, i.e. the results from executing `SELECT * from <table>` on each

`python3 get_tables.py <path_to_log> <output_table_path>`

`python3 get_tables.py logs/…/log{0,1} logs/…/t{0,1}`

----

`table_diff.py`: outputs the line-by-line differences between the two tables compared.

`logs/…/diff`: the diff between the two setups

`logs/…/rt_diff{0,1}`: the diff between that setup and the expected values

`python3 table_diff.py <table0> <table1>`

`python3 table_diff.py logs/…/t0 logs/…/t1` (diff test)

`python3 table_diff.py logs/…/rt logs/…/t1` (round trip test, for rt the first argument must be the rt table)

----

```inspect_result.py```: produces JSON-structured table output, removes false positives, compacts row results and groups based on known discrepancies.

Usage: `python3 inspect_result.py log_dir <interface>` e.g. `python3 inspect_result.py logs/2022.04.16-15.36.49/ hs`

interface is one of `ss`, `hs` and `sh`.

The `expected_tests` are the values generated by SparkSQL (in Spark-Spark) / HiveQL (in Spark-Hive/Hive-Spark).

### Outputs

`<interface>_difft_failed_results.json`: Includes all failed tests for the differential test oracle. Each input should contain more than one output value and for each output value lists all combinations producing that value (out of 12 combinations, `hs` interface will only have 6).
```json
    "0":{ // table number
        "original_value":"cast(-128 as byte)",
        "type":"BYTE",
        "valid":true,
        "output":{
            "-128":[ // output 1
                { // all wr interface and format type combinations that produce output 1
                    "write_interface":"sql",
                    "read_interface":"sql",
                    "format_type":"avro"
                }, \\ ...
            ],
            "No output, find exception: org.apache.spark.sql.avro.IncompatibleSchemaException: Cannot convert Avro type {\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"c0\",\"type\":[\"int\",\"null\"]},{\"name\":\"c1\",\"type\":[\"int\",\"null\"]}]} to SQL type STRUCT<`c0`: INT, `c1`: TINYINT>.":[
                {  // all wr interface and format type combinations that produce output 2
                    "write_interface":"df",
                    "read_interface":"sql",
                    "format_type":"avro",
                    "log_location":"(read) log_w_df_r_sql_avro (line 35)" \\ for erroneous value/error, log location of error
                }, \\ ...
            ]
         } // ...
    
```

`<interface>_wr_failed_results.json`: Includes all failed tests for the write-read test oracle. Each input should have a list of wr interface and format types where they differ in the `write_value` and the `read_value` (up to 12 combinations, 6 in `hs`). The `read_value` could be an error or an erroneous value. 
```json
"0":{
        "t_w_df_r_sql_avro":{ // log file where the test failure was found
            "write_interface":"df",
            "read_interface":"sql",
            "format_type":"avro",
            "read_value":"No output, find exception: org.apache.spark.sql.avro.IncompatibleSchemaException: Cannot convert Avro type {\"type\":\"record\",\"name\":\"topLevelRecord\",\"fields\":[{\"name\":\"c0\",\"type\":[\"int\",\"null\"]},{\"name\":\"c1\",\"type\":[\"int\",\"null\"]}]} to SQL type STRUCT<`c0`: INT, `c1`: TINYINT>.",
            "write_value":"-128",
            "log_location":"(read) log_w_df_r_sql_avro (line 35)", \\ for erroneous value/error, log location of error
            "pass":false
        }, // ...
 ```

`ss_eh_failed.json`: Includes all failed tests for the error handling test oracle (which only operates on invalid values, so only `ss` interface has it). Each input should have a list of wr interfaces and format types where the value is written without an error or warning message. To get the query given for input, you can check the corresponding log file, e.g. `log_w_sql_r_sql_avro` for this case.
```json
"5":{
        "t_w_sql_r_sql_avro":{
            "write_interface":"sql",
            "read_interface":"sql",
            "format_type":"avro",
            "read_value":"null",
            "write_value":"null",
            "pass":false
        },
```

`<interface>_ungrouped_results.json`: JSON-structured table output of each row in all tests (12 combinations), with information about exceptions if no output is given.

```json
{
    "0":{ // table number
        "t_w_df_r_df_avro":{ // table output filename
            "format_type":"avro",
            "read_interface":"df",
            "write_interface":"df",
            "value":"No output, find exception: org.apache.spark.sql.avro.IncompatibleSchemaException",
            "log_location":"(read) log_w_df_r_df_avro (line 60)" // with information about exceptions if could be found
        },
        "t_w_df_r_df_orc":{
            "format_type":"orc",
            "read_interface":"df",
            "write_interface":"df",
            "value":"-128",
        },
        // ...
    },
```
`<interface>_difft_row_compact.json`: More compact results of each row, with original value, expected value (Spark SQL output as a baseline, we may remove this in the future), grouped expected tests and unexpected_tests.

```json
    "0":{ // row number
        "original_value":"cast(-128 as byte)", // the original value to be inserted
        "type":"BYTE",  // Column schema
        "expected_value":"-128", // Spark SQL output as a baseline, we may remove this in the future
        "expected_tests":{ // tests that are consistent with the Spark SQL output (with their configs)
            "-128":[
                {"write_interface":"df",  "read_interface":"df", "format_type":"parquet"},
                {"write_interface":"sql",  "read_interface":"sql", "format_type":"orc"},
                ...
            ]
        },
        "unexpected_tests":{  // tests that are inconsistent with the Spark SQL output (with log location for exceptions if could be found)
            "No output, find exception: org.apache.spark.sql.avro.IncompatibleSchemaException":[
                {"write_interface":"df",  "read_interface":"df", "format_type":"avro",  "log_location":"(read) log_w_df_r_df_avro (line 60)"},
                {"write_interface":"df",  "read_interface":"sql", "format_type":"avro",  "log_location":"(read) log_w_df_r_sql_avro (line 36)"}
            ]
        }
    },
```
