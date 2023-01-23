validate_environment_variables () {
  declare -a env_variables=("SPARK_HOME_E2E" "SPARK_HOME_ONEWAY" "HIVE_HOME" "HADOOP_HOME")
  declare -a env_variable_vals=("$SPARK_HOME_E2E" "$SPARK_HOME_ONEWAY" "$HIVE_HOME" "$HADOOP_HOME")
  for idx in "${!env_variable_vals[@]}"; do
      if [ "${env_variable_vals[$idx]}" == "" ]; then
        printf '%s\n' "Set the ${env_variables[$idx]} environment variable before executing the experiments!" >&2
        exit 1
      fi
      if [[ ! -d "${env_variable_vals[$idx]}" ]]; then
        printf '%s\n' "Set ${env_variables[$idx]} to a VALID DIRECTORY before executing the experiments! Current value: ${env_variable_vals[$idx]}" >&2
        exit 1
      fi
  done
}

delete_table_data () {
  hive_table_data=(/user/hive/warehouse/*s*)
  "$HADOOP_HOME"/bin/hadoop fs -rm -r "${hive_table_data[@]}"
}

start_hive_metastore () {
  nohup "$HIVE_HOME"/bin/hive --service metastore > /dev/null 2>&1 &
  echo "Sleeping for 30 seconds to wait for the Hive metastore to come up."
  sleep 30
  export hms_pid=$!
  echo "Hive Metastore is running with Process ID $hms_pid!"
}

kill_hive_metastore () {
  echo "Killing Hive Metastore with Process ID $hms_pid!"
  kill -9 $hms_pid
}
