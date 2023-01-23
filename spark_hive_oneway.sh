#!/usr/bin/bash
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
spark_sql="$SPARK_HOME"/bin/spark-sql
spark_shell="$SPARK_HOME"/bin/spark-shell


declare -a formats=("parquet" "orc" "avro")

validate_environment_variables
python3 "$value_gen" "$logdir" spark hive --one_way

for format in "${formats[@]}"; do
  export SPARK_HOME="$spark_e2e"
  delete_table_data

  start_hive_metastore
  # Writing data using Spark's SQL Interface (spark-sql)
  < "$logdir"/w_sql_"$format" "$spark_sql" \
     --jars "$cli_jars" \
     --conf spark.sql.hive.metastore.version="$hive_version" \
     --conf spark.sql.hive.metastore.jars="$hive_libs" \
     --conf spark.sql.warehouse.dir="$hive_warehouse_dir" \
     --packages "$cli_packages" 2>&1 | tee "$logdir"/log_w_sql_"$format"
  kill_hive_metastore

  # Reading data written by Spark's spark-sql interface from Hive's HQL interface (HiveCLI)
  < "$logdir"/r_hql_"$format" "$HIVE_HOME"/bin/hive 2>&1 | tee "$logdir"/log_w_sql_r_hql_"$format"

  export SPARK_HOME="$spark_execution_engine"
  # Inserting data into a table created through Spark's SQL shell, through HiveCLI
  < "$logdir"/w_hql_"$format" "$HIVE_HOME"/bin/hive 2>&1 | tee "$logdir"/log_w_sql_w_hql_"$format"

  export SPARK_HOME="$spark_e2e"
  delete_table_data

  start_hive_metastore
  # Writing data using Spark's SQL Interface (spark-shell)
  < "$logdir"/w_df_"$format" "$spark_shell" \
        --jars "$cli_jars" \
        --conf spark.sql.hive.metastore.version="$hive_version" \
        --conf spark.sql.hive.metastore.jars="$hive_libs" \
        --conf spark.sql.warehouse.dir="$hive_warehouse_dir" \
        --packages "$cli_packages" 2>&1 | tee "$logdir"/log_w_df_"$format"
  kill_hive_metastore

  # Reading data written by Spark's spark-shell interface from Hive's HQL interface (HiveCLI)
  < "$logdir"/r_hql_"$format" "$HIVE_HOME"/bin/hive 2>&1 | tee "$logdir"/log_w_df_r_hql_"$format"

  export SPARK_HOME="$spark_execution_engine"
  # Inserting data into a table created through Spark's SQL shell, through HiveCLI
  < "$logdir"/w_hql_"$format" "$HIVE_HOME"/bin/hive 2>&1 | tee "$logdir"/log_w_df_w_hql_"$format"

  python3 "$get_tables" "$logdir"/log_w_df_"$format" "$logdir"/t_w_df_"$format" scala --rt
  python3 "$get_tables" "$logdir"/log_w_sql_"$format" "$logdir"/t_w_sql_"$format" spark-sql --rt
  python3 "$get_tables" "$logdir"/log_w_sql_r_hql_"$format" "$logdir"/t_w_sql_r_hql_"$format" hive
  python3 "$get_tables" "$logdir"/log_w_df_r_hql_"$format" "$logdir"/t_w_df_r_hql_"$format" hive
done

python3 inspect_result.py "$logdir"/ sh
export SPARK_HOME="$spark_home"
