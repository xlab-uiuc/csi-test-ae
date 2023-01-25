#!/bin/sh

# Install dependencies
apt-get update && \
    apt-get install -y openjdk-8-jdk maven python3 python3-pip ssh rsync

# Hadoop install
curl https://archive.apache.org/dist/hadoop/core/hadoop-3.3.2/hadoop-3.3.2.tar.gz | tar xz -C .
mv hadoop-3.3.2 hadoop
cd hadoop
export HADOOP_HOME=$(pwd)

# Set up Hadoop configs from the config files
cp ../conf/hadoop-env.sh etc/hadoop/hadoop-env.sh
cp ../conf/core-site.xml etc/hadoop/core-site.xml
cp ../conf/hdfs-site.xml etc/hadoop/hdfs-site.xml

# Set up localhost ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Start HDFS nodes
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh

# Absolute paths for python scripts that are invoked during the experiments
get_tables="$script_dir"/get_tables.py
table_diff="$script_dir"/table_diff.py
test_failures="$script_dir"/test_failures.py

cd ..

# Spark setup for one way experiments
git clone https://github.com/apache/spark spark-hive
cd spark-hive
export SPARK_HOME=$(pwd)

export SPARK_DIST_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
mkdir /tmp/spark-events

# Hive is only compatible with Spark with version <= 2.3.0
git checkout v2.3.0
./build/mvn -Dhadoop.version=3.3.2 -Pyarn -Phive -Phive-thriftserver -DskipTests clean install

# Find Hive jars bundled with Spark
find $SPARK_HOME/assembly/target/scala-2.11/jars -name "*hive*.jar"

# Spark 2.3.0 comes bundled with old Hive jars (1.2.1 in my case), we need to
# delete them. This is because we will deploy Hive 3.1.2.
# This step might require you to change 1.2.1 to the version that you see in the
# output of the above find command.
rm $SPARK_HOME/assembly/target/scala-2.11/jars/hive*1.2.1*

# Start Spark Master & Worker, Hive contacts these services to submit query jobs
$SPARK_HOME/sbin/start-all.sh

# Required for running the experiments.
export SPARK_HOME_ONEWAY=$(pwd)

# Hive setup
cd ..
$HADOOP_HOME/bin/hadoop fs -mkdir /tmp
$HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp
$HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
$HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse
wget https://archive.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz && \
	tar xzvf apache-hive-3.1.2-bin.tar.gz
mv apache-hive-3.1.2-bin hive && cd hive
export HIVE_HOME=$(pwd)

# Link Spark libraries to Hive's lib folder, this is required if Spark is to be
# used as the execution engine for Hive.
cd $HIVE_HOME/lib/
ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/scala-library*.jar
ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-core*.jar
ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-network-common*.jar
ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-network-shuffle*.jar

# Set up Hive configs
cd ..
cp ../conf/hive-site.xml conf/hive-site.xml

$HIVE_HOME/bin/schematool -dbType derby -initSchema

# Spark E2E setup
cd ..
git clone https://github.com/apache/spark
cd spark
export SPARK_HOME_E2E=$(pwd)
git checkout v3.2.1
./build/mvn -Dhadoop.version=3.3.2 -Pyarn -Phive -Phive-thriftserver -DskipTests clean install

# Set up Spark configs
cp ../conf/spark-hive-site.xml conf/hive-site.xml
