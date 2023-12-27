import pytest
from pyspark.sql import SparkSession

# Define a fixture to create a SparkSession for testing
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()

# Define a test function for the S3 initial and delta load job
def test_s3_initial_delta_load(spark_session):
    # Read sample test data (can be replaced with your test data)
    test_data = [
        ('Alice', 25),
        ('Bob', 30),
        ('Charlie', 22)
    ]
    columns = ['name', 'age']
    test_df = spark_session.createDataFrame(test_data, columns)

    # Replace these paths with your test paths or use local directories
    test_input_path = "s3://your_test_bucket/test_input_folder/*.json"
    test_output_path = "s3://your_test_bucket/test_output_folder"

    # Process test data (initial load)
    process_data(test_df, is_initial_load=True, output_path=test_output_path)

    # Read processed data to validate
    processed_data = spark_session.read.parquet(test_output_path)

    # Assert that processed data contains the expected columns
    assert set(processed_data.columns) == {'name', 'age'}

    # Add more assertions as needed to validate the processed data
    # For instance, you can check for specific values or row counts
    assert processed_data.count() == len(test_data)
