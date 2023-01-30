#!/bin/bash
clear

ts=$(date "+%Y.%m.%d-%H.%M.%S")

# Absolute path of the directory where this script resides. Used to make sure this script can be run from anywhere.
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Importing constants and utility methods
source "$script_dir"/utils.sh

# Setting up the directory where experiment logs would be dumped
filename=$(basename -- "$0")
logdir="$script_dir"/logs/${filename%".sh"}/"$ts"
mkdir -p "$logdir"

spark_home="$SPARK_HOME"
spark_e2e="$SPARK_HOME_E2E"

# Absolute paths for python scripts that are invoked during the experiments
get_tables="$script_dir"/get_tables.py
table_diff="$script_dir"/table_diff.py
test_failures="$script_dir"/test_failures.py

# Absolute paths for Spark CLIs
spark_sql="$SPARK_HOME_E2E"/bin/spark-sql
spark_shell="$SPARK_HOME_E2E"/bin/spark-shell

avro_package="org.apache.spark:spark-avro_2.12:3.2.1"
declare -a formats=("parquet" "orc" "avro")
declare -a ifs=("sql" "df")

validate_environment_variables

export SPARK_HOME="$spark_e2e"
python3 "$script_dir"/value_gen.py "$logdir" spark spark

start_hive_metastore
for format in "${formats[@]}"; do
  delete_table_data
  
  # Writing table data using Spark's SQL Interface (spark-sql)
  < "$logdir"/w_sql_"$format" "$spark_sql" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_sql_"$format"

  # Reading data written using Spark's spark-sql using Spark's spark-sql interface
  < "$logdir"/r_sql_"$format" "$spark_sql" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_sql_r_sql_"$format"

  # Reading data written using Spark's spark-sql using Spark's spark-shell interface
  < "$logdir"/r_df_"$format" "$spark_shell" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_sql_r_df_"$format"

  delete_table_data

  # Writing table data using Spark's SQL Interface (spark-shell)
  < "$logdir"/w_df_"$format" "$spark_shell" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_df_"$format"

  # Reading data written using Spark's spark-shell using Spark's spark-sql interface
  < "$logdir"/r_sql_"$format" "$spark_sql" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_df_r_sql_"$format"

  # Reading data written using Spark's spark-shell using Spark's spark-shell interface
  < "$logdir"/r_df_"$format" "$spark_shell" --packages "$avro_package" 2>&1 | tee "$logdir"/log_w_df_r_df_"$format"

  python3 "$get_tables" "$logdir"/log_w_sql_"$format" "$logdir"/t_w_sql_"$format" spark-sql --rt
  python3 "$get_tables" "$logdir"/log_w_df_"$format" "$logdir"/t_w_df_"$format" scala --rt

  python3 "$get_tables" "$logdir"/log_w_sql_r_sql_"$format" "$logdir"/t_w_sql_r_sql_"$format" spark-sql
  python3 "$get_tables" "$logdir"/log_w_sql_r_df_"$format" "$logdir"/t_w_sql_r_df_"$format" scala
  python3 "$get_tables" "$logdir"/log_w_df_r_sql_"$format" "$logdir"/t_w_df_r_sql_"$format" spark-sql
  python3 "$get_tables" "$logdir"/log_w_df_r_df_"$format" "$logdir"/t_w_df_r_df_"$format" scala
done
kill_hive_metastore
delete_table_data

python3 inspect_result.py "$logdir"/ ss
export SPARK_HOME="$spark_home"
