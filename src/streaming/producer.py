#Create a Spark Structured Streaming (Python or Scala) Pipeline to publish some data to Kafka
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.getBuilder.appName("PublishtoKafka").getOrCreate()

# Define the Kafka broker and topic
KAFKA_BOOTSTRAP_SERVERS = sys.argv[1] 
KAFKA_TOPIC = sys.argv[2] 

# Create a sample DataFrame with some data
data = [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
#define custom_schema
custom_schema = StructType([StructField("key", StringType(), True),
                StructField("value", StringType(), True)])
df = spark.createDataFrame(data, custom_schema)

#write data to kafka topic using writeStream API and before that convert to json aswell.
try:
    result_stream = df.selectExpr("CAST(key as STRING)", "to_json()) as value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .option("checkpointLocation", "/kafka_checkpoint_dir").start()
except Exception as e:
    print(f"Error writing : {e}")

#wait until streaming query to complete
try:
    result_stream.awaitTermination()
except KeyboardInterrupt:
    # Free up cluster resources
    spark.stop()
    pass
