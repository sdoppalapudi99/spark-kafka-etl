#Create a Spark Structured Streaming (Python or Scala) Pipeline to publish some data to Kafka
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaPublisher") \
    .getOrCreate()

# Define the Kafka broker and topic
KAFKA_BOOTSTRAP_SERVERS = sys.argv[1] 
KAFKA_TOPIC = sys.argv[2] 

# Create a sample DataFrame with some data
data = [("John", 25), ("Alice", 30), ("Bob", 28)]
schema = ["name", "age"]
df = spark.createDataFrame(data, schema)

# Convert DataFrame to JSON and select required columns
df_json = df.select(to_json(struct("*")).alias("value"))

# Write data to Kafka in a streaming manner
kafka_query = df_json \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("topic", KAFKA_TOPIC) \
    .start()

# Wait for the streaming query to finish
kafka_query.awaitTermination()
