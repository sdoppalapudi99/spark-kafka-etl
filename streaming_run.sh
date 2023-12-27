#!/bin/bash

# Set Spark home directory
export SPARK_HOME=/path/to/your/spark

# Set Python executable (if needed)
export PYSPARK_PYTHON=/path/to/your/python


KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092"
KAFKA_TOPIC = "test_topic"

# Your PySpark application file
PRODUCER_PYSPARK_FILE=streaming/producer.py
CONSUMER_PYSPARK_FILE=streaming/consumer.py

# Spark cluster URL
SPARK_MASTER_URL=spark://your_spark_master:port

# Additional configurations if needed
SPARK_CONFIGS="--num-executors 4 --executor-memory 4G --driver-memory 2G"

# Submit the PySpark application in cluster mode
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode cluster \
    $SPARK_CONFIGS \
    $PRODUCER_PYSPARK_FILE $KAFKA_BOOTSTRAP_SERVERS $KAFKA_TOPIC

sleep 10

# Submit the PySpark application in cluster mode
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER_URL \
    --deploy-mode cluster \
    $SPARK_CONFIGS \
    $CONSUMER_PYSPARK_FILE $KAFKA_BOOTSTRAP_SERVERS $KAFKA_TOPIC
