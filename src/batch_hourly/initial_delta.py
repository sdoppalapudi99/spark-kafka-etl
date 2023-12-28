#Reads data from RAW Zone using an Hourly scheduled Spark Batch process and loads the final parquet
#file – (Processed Zone)
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# True for initial and False for delta load
is_initial_load=True

# Define your S3 bucket and folder paths
s3_raw_bucket = "ns-s3-raw-bucket"
s3_processed_bucket = "ns-s3-processed-bucket"
s3_raw_path = "s3://{}/your_input_folder".format(s3_raw_bucket)  # Input folder containing JSON files

#get hourly schedule last hour batch
hourly = datetime.now() - timedelta(hours=1)
schedule_time = hourly.strftime("y=%Y/m=%m/d=%d/h=%H")
s3_processed_path = "s3://{}/your_processed_folder".format(s3_processed_bucket)  # Folder for processed Parquet files

# Create a Spark session
spark = SparkSession.builder \
    .appName("S3InitialDeltaLoadJob") \
    .getOrCreate()

# Read data from S3
data = spark.read.format("delta").load(f"{s3_raw_path}/{schedule_time}")

# Define function for data processing and handling
def process_data(df, is_initial_load=False):

    # If it's an initial load, overwrite the entire dataset; else, perform delta load
    if is_initial_load:
        df.write.mode("overwrite").parquet(s3_processed_path)
    else:
        # Load existing data and union with new data for delta
        existing_df = spark.read.parquet(s3_processed_path)
        final_df = existing_df.union(df)
        final_df.write.mode("overwrite").parquet(s3_processed_path)
try:
    # Process and handle the data for initial and delta loads
    process_data(data, is_initial_load)  # Initial load
except (Exception, TypeError) as error:
    print(error)

# Stop the Spark session and # Free up cluster resources
spark.stop()
