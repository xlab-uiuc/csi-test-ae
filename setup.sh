#!/bin/bash

# Start HDFS nodes
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh

# Spark setup for one way experiments
# Start Spark Master & Worker, Hive contacts these services to submit query jobs
$SPARK_HOME_ONEWAY/sbin/start-all.sh

# Hive HDFS setup
$HADOOP_HOME/bin/hadoop fs -mkdir /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse
