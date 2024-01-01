#!/bin/bash

# Set Spark home directory
export SPARK_HOME=/path/to/your/spark

# Set Python executable (if needed)
export PYSPARK_PYTHON=/path/to/your/python

# Your PySpark application file
ETL_PYSPARK_FILE=src/streaming/batch_hourly.py

# Spark cluster URL or Yarn
SPARK_MASTER_URL=spark://your_spark_master:port

# Additional configurations if needed
SPARK_CONFIGS="--num-executors 4 --executor-memory 4G --driver-memory 2G"

# Submit the PySpark application in cluster/client mode 
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode cluster \
    $SPARK_CONFIGS \
    $ETL_PYSPARK_FILE
