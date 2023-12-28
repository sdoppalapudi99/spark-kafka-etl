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

restart_from_offset = sys.argv[3] 

s3_raw_bucket = "ns-s3-raw-bucket"
s3_raw_path = "s3://{}/your_input_folder".format(s3_raw_bucket) 

# Define the schema for the XML data
xml_custom_schema = StructType() .add("col1", IntegerType()) \
    .add("col2", IntegerType()) \
    .add("col3", StringType())

# Define function for data processing and handling
def process_data(df):
    # Schema validation
    validated_df = df.select(from_xml(col("value").cast("string"), xml_custom_schema).alias("data")) \
                     .filter("data IS NOT NULL") \
                     .select("data.*")

    # Data type validation and processing
    processed_df = validated_df.filter("key IS NOT NULL AND value IS NOT NULL")

    # Deduplication based on a unique identifier, assuming 'key' as the unique identifier
    deduplicated_df = processed_df.dropDuplicates(["key"])

    # Perform your operations on the deduplicated data
    #deduplicated_df.show(truncate=False)

# Load data from Kafka in a streaming manner
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Process and handle the data
if restart_from_offset != "latest":
    query = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreachBatch(process_data) \
        .option("checkpointLocation", "/path/to/checkpoint/folder") \
        .format("parquet") \
        .option("path", s3_raw_path) \
        .partitionBy("date") \
        .outputMode("append") \
        .start()
else:
    query = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreachBatch(process_data) \
        .option("startingOffsets", restart_from_offset) \
        .option("checkpointLocation", "/path/to/checkpoint/folder") \
        .format("parquet") \
        .option("path", s3_raw_path) \
        .partitionBy("date") \
        .outputMode("append") \
        .start()

# Exception handling and restart from a specific offset
try:
    query.awaitTermination()
except KeyboardInterrupt:
    # Free up cluster resources
    query.stop()
    spark.stop()
    pass

