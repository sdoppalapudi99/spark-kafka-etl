Spark streaming example for Kafka: pseudo code not a working example

##Scripts

streaming_run.sh  : Starts the producer spark job, waits 10 sec and start the consumer job  

    This script use 
        streaming/producer.py : writes sample data to kafka
        streaming/consumer.py : consumes data from Kafka and write to S3 in parquet format

batch_run.sh  : Starts the ETL job for raw zone to processed zone  

    This script use  
        batch_hourly/initial_delta.py : writes raw bucket location to processed bucket location
