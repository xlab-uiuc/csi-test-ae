## Cross-System Testing Case Study: Spark-Hive Data Plane

Running the experiments here can reproduce the test failures which reflect the discrepancies and reported issues discussed in Section 8 of the EuroSys '23 paper. 

## Getting started
### Requirements
- Tested on Ubuntu 18/20 i7-11700 @ 2.50GHz, OS X
- Total time: for installation, ~2 hours for all experiments

Dependencies
```commandline
apt-get update && \
    apt-get install -y openjdk-8-jdk maven python3 python3-pip ssh rsync
```

### Setup
Run setup script or directly use Docker setup

You should expect Spark, Hive, and Hadoop to be installed after the setup.

TODO: changing $HOMEs

TODO: adding information about testing to see if they are running

## Reproducing the experiments
### Scripts

`spark_e2e.sh`: runs the Spark-Spark testing

`spark_hive_oneway.sh`: runs Spark-Hive testing

`hive_spark_oneway.sh`: runs Hive-Spark testing

For detailed usage of any intermediate scripts, see [scripts_usage.md](scripts_usage.md).

The test failures observed in `_failed.json` should include those indicated in the table below.
Note that some test failures may appear multiple times in the table since the same test can exhibit more than one of the following behaviors or conditions.
The test failures in the table are not comprehensive, omitted tests may be caused by similar root causes or exhibit similar behavior as included tests.



<!---
Dependencies
```commandline
apt-get update && \
    apt-get install -y openjdk-8-jdk maven python3 python3-pip ssh rsync
```


### Setting up the Spark-Hive-Hadoop infrastructure
**Hadoop setup:**
```commandline
curl https://archive.apache.org/dist/hadoop/core/hadoop-3.3.2/hadoop-3.3.2.tar.gz | tar xz -C .
mv hadoop-3.3.2 hadoop
cd hadoop
export HADOOP_HOME=$(pwd)
```
Add to `etc/hadoop/hadoop-env.sh` your `JAVA_HOME` for Java 8 and replace `USERID` with your username, e.g.
```
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export HDFS_NAMENODE_USER=USERID
export HDFS_DATANODE_USER=USERID
export HDFS_SECONDARYNAMENODE_USER=USERID
export YARN_RESOURCEMANAGER_USER=USERID
export YARN_NODEMANAGER_USER=USERID
```
Update the following config files

`etc/hadoop/core-site.xml`
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```
`etc/hadoop/hdfs-site.xml`
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```
If you cannot `ssh localhost`:
```commandline
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

Might need `sudo`
```commandline
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh
```

**Spark setup (to use as Hive's execution engine):**
```commandline
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
```

**Hive setup:**

First, execute the below commands to setup the basic Hive environment:
```commandline
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
```
Next, create `hive-site.xml` using `vim $HIVE_HOME/conf/hive-site.xml`:
1. Replace `$HADOOP_HOME` with the hadoop path on your machine
2. The value for spark.master must be fetched from http://<Host-IP>:8080 (Spark master UI)
```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hive.execution.engine</name>
        <value>spark</value>
        <description>Use Spark as Hive's execution engine</description>
    </property>
    <property>
        <name>spark.master</name>
        <value></value>
        <description>Fetch this from http://Host-IP:8080 (Spark Master UI)</description>
    </property>
    <property>
        <name>spark.eventLog.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>spark.eventLog.dir</name>
        <value>/tmp/spark-events</value>
    </property>
    <property>
        <name>spark.serializer</name>
        <value>org.apache.spark.serializer.KryoSerializer</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:derby:$HADOOP_HOME/metastore_db;create=true</value>
        <description>JDBC connect string for a JDBC metastore</description>
    </property>
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>hdfs://localhost:9000/user/hive/warehouse</value>
        <description>Warehouse Location</description>
    </property>
</configuration>
```

Lastly, execute the command below to configure the Hive metastore DB backed by Derby. This
command will initialise the DB under $HADOOP_HOME/metastore_db.
```commandline
$HIVE_HOME/bin/schematool -dbType derby -initSchema
```

**Spark setup:**
```commandline
git clone https://github.com/apache/spark
cd spark
export SPARK_HOME_E2E=$(pwd)
git checkout v3.2.1
./build/mvn -Dhadoop.version=3.3.2 -Pyarn -Phive -Phive-thriftserver -DskipTests clean install
```
Create `conf/hive-site.xml`
```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://localhost:9083</value>
  </property>
</configuration>
```
----

## Scripts

`spark_e2e.sh`: runs the Spark-Spark testing

`spark_hive_oneway.sh`: runs Spark-Hive testing

`hive_spark_oneway.sh`: runs Hive-Spark testing

For detailed usage of any intermediate scripts, see [scripts_usage.md](scripts_usage.md).

## Using the Spark shells

`./bin/spark-sql`: can directly enter SQL queries into this shell, use for testing SQL interface

`./bin/spark-sql --packages org.apache.spark:spark-avro_2.12:3.2.1` for e.g. to include packages such as avro serde format

`./bin/spark-shell`: a scala shell with the spark dependencies already loaded, use for DataFrame and Dataset

### SQL example to test values:

To specify the types when constructing:
 ```
CREATE TABLE t0(c0 INT, c1 FLOAT)
INSERT INTO t0 VALUES (0, 1.0 * pi())
SELECT * FROM t0
```

Can also do
`SELECT 1.0 * pi()`
`SELECT CAST(1 AS DECIMAL(4,2))`.
Types will be inferred based on constants provided.
Note that `CAST` has different semantics from directly defining the value to have a specific type.

### DataFrame example to test values:
```
val df = Seq(-1.0/0, 0.0/0, 1.0 * math.Pi).toDF(“value”)
df.show(false)
df.selectExpr(“CAST(value AS float)")
// “value” is a tag that can be used for SQL querying`
// false to avoid truncation`
```

The type will be inferred based on the constants provided. In this case it is inferred as `double`.

Note that `CAST` has different semantics from directly defining the value to have a specific type. The default behavior usually will output `NULL` in situations where directly defining the value would give an error, and this behavior is configurable.

To construct a DataFrame with a specific type, use the `createDataFrame()` API. Steps TODO

-->
