#!/bin/bash
clear

ts=$(date "+%Y.%m.%d-%H.%M.%S")

# Absolute path of the directory where this script resides. Used to make sure this script can be run from anywhere.
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

spark_home="$SPARK_HOME"
spark_execution_engine="$SPARK_HOME_ONEWAY"
spark_e2e="$SPARK_HOME_E2E"
# Importing constants and utility methods
source "$script_dir"/setup_constants_oneway.sh
source "$script_dir"/utils.sh

# Setting up the directory where experiment logs would be dumped
filename=$(basename -- "$0")
logdir="$script_dir"/logs/${filename%".sh"}/"$ts"
mkdir -p "$logdir"

# Absolute paths for python scripts that are invoked during the experiments
value_gen="$script_dir"/value_gen.py
get_tables="$script_dir"/get_tables.py


# Absolute paths for Spark CLIs
spark_sql="$SPARK_HOME_E2E"/bin/spark-sql
spark_shell="$SPARK_HOME_E2E"/bin/spark-shell

declare -a formats=("orc" "avro" "parquet")
validate_environment_variables
python3 "$value_gen" "$logdir" hive spark --one_way


for format in "${formats[@]}"; do
  delete_table_data

  export SPARK_HOME="$spark_execution_engine"
  # Wring data using Hive's HQL Interface (Hive CLI)
  < "$logdir"/w_hql_"$format" "$HIVE_HOME"/bin/hive 2>&1 | tee "$logdir"/log_w_hql_"$format"
  python3 "$get_tables" "$logdir"/log_w_hql_"$format" "$logdir"/t_w_hql_"$format" hive --rt

  export SPARK_HOME="$spark_e2e"
  start_hive_metastore
  # Reading data written using Hive's HQL from Spark's SQL interface (spark-sql)
  < "$logdir"/r_sql_"$format" "$spark_sql" \
  --jars "$cli_jars" \
  --conf spark.sql.hive.metastore.version="$hive_version" \
  --conf spark.sql.hive.metastore.jars="$hive_libs" \
  --conf spark.sql.warehouse.dir="$hive_warehouse_dir" \
  --packages "$cli_packages" 2>&1 | tee "$logdir"/log_w_hql_r_sql_"$format"
  python3 "$get_tables" "$logdir"/log_w_hql_r_sql_"$format" "$logdir"/t_w_hql_r_sql_"$format" spark-sql


  # Reading data written using Hive's HQL from Spark's DF interface (spark-shell)
  < "$logdir"/r_df_"$format" "$spark_shell" \
  --jars "$cli_jars" \
  --conf spark.sql.hive.metastore.version="$hive_version" \
  --conf spark.sql.hive.metastore.jars="$hive_libs" \
  --conf spark.sql.warehouse.dir="$hive_warehouse_dir" \
  --packages "$cli_packages" 2>&1 | tee "$logdir"/log_w_hql_r_df_"$format"
  python3 "$get_tables" "$logdir"/log_w_hql_r_df_"$format" "$logdir"/t_w_hql_r_df_"$format" scala

  # Writing to table created using Hive's HQL from Spark's SQL interface (spark-sql)
  < "$logdir"/w_sql_"$format" "$spark_sql" \
  --jars "$cli_jars" \
  --conf spark.sql.hive.metastore.version="$hive_version" \
  --conf spark.sql.hive.metastore.jars="$hive_libs" \
  --conf spark.sql.warehouse.dir="$hive_warehouse_dir" \
  --packages "$cli_packages" 2>&1 | tee "$logdir"/log_w_hql_w_sql_"$format"

  kill_hive_metastore
done

python3 inspect_result.py "$logdir"/ hs
export SPARK_HOME="$spark_home"
