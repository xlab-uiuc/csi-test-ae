## Cross-System Testing Case Study: Spark-Hive Data Plane

Running the experiments here can reproduce the test failures which reflect the discrepancies and reported issues discussed in Section 8 of the EuroSys '23 paper. 

## Getting started (~1 hour)
### Requirements
- Tested on Ubuntu 20 i7-11700 @ 2.50GHz, OS X
- Total time: ~1 hour for installation, ~2 hours for all experiments

### Setup
The easiest way to run the experiments is to pull the Docker container:
```bash
docker pull chaitanyabhandari/csi-eurosys23-ae:linux-amd64
```
or
```bash
docker pull chaitanyabhandari/csi-eurosys23-ae:linux-arm64-v8
```
depending on your architecture (`dpkg --print-architecture`).

Then run the Docker container:
```bash
docker run -it chaitanyabhandari/csi-eurosys23-ae:linux-amd64
```
or
```bash
docker run -it chaitanyabhandari/csi-eurosys23-ae:linux-arm64-v8
```

You should expect two Spark instances, Hive, and Hadoop to be installed after the setup.

TODO: adding information about testing to see if they are running

## Reproducing the experiments (~ 2-3 hours)

To execute the experiments
```bash
./spark_e2e.sh
./spark_hive_oneway.sh
./hive_spark_oneway.sh
```

### Scripts

`spark_e2e.sh`: runs the Spark-Spark testing

`spark_hive_oneway.sh`: runs Spark-Hive testing

`hive_spark_oneway.sh`: runs Hive-Spark testing

For detailed usage of any intermediate scripts, see [scripts_usage.md](scripts_usage.md).
