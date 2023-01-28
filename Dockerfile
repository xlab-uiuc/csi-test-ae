# Fetch ubuntu 18.04 LTS docker image
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
        apt-get -y install sudo

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential\
        expect git vim zip unzip wget openjdk-8-jdk wget maven curl && \
    apt-get install -y python3 python3-pip ssh

RUN echo '#! /bin/sh' > /usr/bin/mesg && \
  chmod 755 /usr/bin/mesg

################################################################################
####################   Tool stuff   ###########################################
################################################################################

# Set relevant environment variables to simplify usage of spark

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
RUN useradd -d /home/csiuser -m csiuser --shell /bin/bash

RUN update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN git clone https://github.com/xlab-uiuc/csi-test-ae.git

ENV MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"

ENV HADOOP_HOME=/csi-test-ae/hadoop
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV YARN_HOME=$HADOOP_HOME
ENV HADOOP_INSTALL=$HADOOP_HOME
ENV HADOOP_CONF_DIR=$HADOOP_HOME
ENV HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec
ENV JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_INSTALL=$HADOOP_HOME

ENV SPARK_HOME_ONEWAY=/csi-test-ae/spark-hive
ENV SPARK_HOME_E2E=/csi-test-ae/spark
ENV HIVE_HOME=/csi-test-ae/hive

ENV PATH=$PATH:$HADOOP_HOME/bin

WORKDIR /csi-test-ae

# Spark setup for one way experiments
RUN git clone https://github.com/apache/spark spark-hive && \
    cd spark-hive

# ENV SPARK_DIST_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
RUN mkdir /tmp/spark-events && \
    cd spark-hive && \
    # Hive is only compatible with Spark with version <= 2.3.0
    git checkout v2.3.0 && \
    export SPARK_HOME=$(pwd) && \
    ./build/mvn -Dhadoop.version=3.3.2 -Pyarn -Phive -Phive-thriftserver -DskipTests clean install && \
    # Find Hive jars bundled with Spark
    find $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars -name "*hive*.jar" && \
    # Spark 2.3.0 comes bundled with old Hive jars (1.2.1 in my case), we need to
    # delete them. This is because we will deploy Hive 3.1.2.
    # This step might require you to change 1.2.1 to the version that you see in the
    # output of the above find command.
    rm $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/hive*1.2.1*

# Spark E2E setup
RUN git clone https://github.com/apache/spark && \
    cd spark && \
    git checkout v3.2.1 && \
    export SPARK_HOME=$(pwd) && \
    ./build/mvn -Dhadoop.version=3.3.2 -Pyarn -Phive -Phive-thriftserver -DskipTests clean install

# Set up Spark configs
ADD conf/spark-hive-site.xml $SPARK_HOME_E2E/conf/hive-site.xml

# Hadoop install
# SSH without key
RUN mkdir /root/.ssh && \
    ssh-keygen -t rsa -f /root/.ssh/id_rsa -P '' && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys
# Set up Hadoop configs from the config files
RUN curl https://archive.apache.org/dist/hadoop/core/hadoop-3.3.2/hadoop-3.3.2.tar.gz | tar xz -C . && \
    mv hadoop-3.3.2 hadoop && \
    cd hadoop

ADD conf/hadoop-env.sh $HADOOP_HOME/etc/hadoop
ADD conf/core-site.xml $HADOOP_HOME/etc/hadoop
ADD conf/hdfs-site.xml $HADOOP_HOME/etc/hadoop
ADD conf/yarn-site.xml $HADOOP_HOME/etc/hadoop
ADD conf/mapred-site.xml $HADOOP_HOME/etc/hadoop

RUN wget https://archive.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz && \
    tar xzvf apache-hive-3.1.2-bin.tar.gz && \
    mv apache-hive-3.1.2-bin hive

ADD conf/spark-hive-site.xml $SPARK_HOME_E2E/conf/hive-site.xml

    # Link Spark libraries to Hive's lib folder, this is required if Spark is to be
    # used as the execution engine for Hive.
RUN cd $HIVE_HOME/lib/ && \
    ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/scala-library*.jar && \
    ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-core*.jar && \
    ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-network-common*.jar && \
    ln -s $SPARK_HOME_ONEWAY/assembly/target/scala-2.11/jars/spark-network-shuffle*.jar

ADD conf/hive-site.xml $HIVE_HOME/conf/hive-site.xml
RUN $HIVE_HOME/bin/schematool -dbType derby -initSchema
EXPOSE 22 8020 8021 9000 9083

ADD setup.sh /csi-test-ae/setup.sh

ENV LANG=en_US.UTF-8

ENTRYPOINT service ssh start && ./setup.sh && /bin/bash
