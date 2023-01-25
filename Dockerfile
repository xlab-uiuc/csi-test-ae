# Fetch ubuntu 18.04 LTS docker image
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
        apt-get -y install sudo

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential\
        expect git vim zip unzip wget openjdk-8-jdk wget maven sudo curl
RUN apt-get install -y python3 python3-pip ssh pdsh

################################################################################
####################   Spark stuff   ###########################################
################################################################################

# Set relevant environment variables to simplify usage of spark

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
RUN useradd -m csiuser
WORKDIR /home/csiuser

RUN update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN cd /home/csiuser && \
    git clone https://github.com/xlab-uiuc/csi-test-ae.git && \
    cd csi-test-ae && \
    export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"

COPY setup.sh /home/csiuser/csi-test-ae/setup.sh

ENV HADOOP_HOME /home/csiuser/csi-test-ae/hadoop
ENV HADOOP_COMMON_HOME=/home/csiuser/csi-test-ae/hadoop
ENV HADOOP_HDFS_HOME=/home/csiuser/csi-test-ae/hadoop
ENV HADOOP_YARN_HOME=/home/csiuser/csi-test-ae/hadoop
ENV HADOOP_MAPRED_HOME=/home/csiuser/csi-test-ae/hadoop
ENV SPARK_HOME_ONEWAY=/home/csiuser/csi-test-ae/spark-hive
ENV SPARK_HOME_E2E=/home/csiuser/csi-test-ae/spark
ENV HIVE_HOME=/home/csiuser/csi-test-ae/hive

ENV PATH=$PATH:/home/csiuser/csi-test-ae/hadoop/bin
ENV PATH=$PATH:/home/csiuser/csi-test-ae/hadoop/sbin
RUN cd /home/csiuser/csi-test-ae && \
    /bin/bash ./setup.sh
