#Reads data from RAW Zone using an Hourly scheduled Spark Batch process and loads the final parquet
#file – (Processed Zone)
from pyspark.sql import SparkSession
from datetime import datetime

# True for initial and False for delta load
is_initial_load=True

# Define your S3 bucket and folder paths
s3_bucket = "your_s3_bucket"
s3_input_path = "s3://{}/your_input_folder".format(s3_bucket)  # Input folder containing JSON files
s3_processed_folder = "s3://{}/your_processed_folder".format(s3_bucket)  # Folder for processed Parquet files

# Create a Spark session
spark = SparkSession.builder \
    .appName("S3InitialDeltaLoadJob") \
    .getOrCreate()

# Read data from S3
data = spark.read.format("delta").load(s3_input_path)

# Define function for data processing and handling
def process_data(df, is_initial_load=False):

    # If it's an initial load, overwrite the entire dataset; else, perform delta load
    if is_initial_load:
        df.write.mode("overwrite").parquet(s3_processed_folder)
    else:
        # Load existing data and union with new data for delta
        existing_df = spark.read.parquet(s3_processed_folder)
        final_df = existing_df.union(df)
        final_df.write.mode("overwrite").parquet(s3_processed_folder)

# Process and handle the data for initial and delta loads
process_data(data, is_initial_load)  # Initial load

# Stop the Spark session
spark.stop()
