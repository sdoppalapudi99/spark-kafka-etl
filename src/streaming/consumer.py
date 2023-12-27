# Using Spark Structured Streaming (Python or Scala) read the same data from Kafka and store it in
# HDFS Parquet - RAW Zone (use any sample XML with nested elements)
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.streaming import DataStreamReader

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Define the Kafka broker and topic
KAFKA_BOOTSTRAP_SERVERS = sys.argv[1] 
KAFKA_TOPIC = sys.argv[2] 
OFFSET = "earliest" # change this for loading from a checkpoint offset

# Define the schema for the incoming data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Define function for data processing and handling
def process_data(df):
    # Schema validation
    validated_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                     .filter("data IS NOT NULL") \
                     .select("data.*")

    # Data type validation and processing
    processed_df = validated_df.filter("name IS NOT NULL AND age IS NOT NULL")

    # Deduplication based on a unique identifier, assuming 'name' as the unique identifier
    deduplicated_df = processed_df.dropDuplicates(["name"])

    # Perform your operations on the deduplicated data
    #deduplicated_df.show(truncate=False)

     # Write processed data to S3 in Parquet format
    processed_df.write \
        .mode("append") \
        .parquet("s3://your_bucket/path/to/output/folder")

# Load data from Kafka in a streaming manner
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Process and handle the data
query = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .foreachBatch(process_data) \
    .option("checkpointLocation", "s3://your_bucket/path/to/checkpoint/folder") \
    .start()

# Exception handling and restart from a specific offset
try:
    query.awaitTermination()
except KeyboardInterrupt:
    # Perform any cleanup operations
    pass
finally:
    # Restart from a specific offset
    spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", ) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .save()
