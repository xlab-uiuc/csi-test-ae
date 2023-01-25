# Fetch ubuntu 18.04 LTS docker image
FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV PYSPARK_PYTHON=python3

RUN apt-get update && \
        apt-get -y install sudo

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential\
        expect git vim zip unzip wget openjdk-8-jdk wget maven sudo curl
RUN apt-get install -y python3 python3-pip

################################################################################
####################   Spark stuff   ###########################################
################################################################################

# Set relevant environment variables to simplify usage of spark

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN useradd -m csiuser
WORKDIR /home/csiuser

RUN update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN cd /home/csiuser && \
    git clone https://github.com/xlab-uiuc/csi-test-ae.git && \
    cd csi-test-ae && \
    export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g" && \
    chmod +x setup.sh && \
    ./setup.sh

RUN export SPARK_HOME_ONEWAY=/home/csiuser/csi-test-ae/spark-hive/ && \
    export SPARK_HOME_E2E=/home/csiuser/csi-test-ae/spark/ && \
    export HIVE_HOME=/home/csiuser/csi-test-ae/hive/ && \
    export HADOOP_HOME=/home/csiuser/csi-test-ae/hadoop

