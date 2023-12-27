from unittest.mock import patch, MagicMock
import pytest
from your_producer_module import publish_to_kafka  # Import your producer function

# Mocked Kafka broker details
kafka_broker = "mock_kafka_broker:port"
kafka_topic = "test_topic"

def test_producer_publishes_data():
    # Patching the Kafka producer function
    with patch('your_producer_module.publish_to_kafka') as mock_producer:
        # Simulate publishing data
        sample_data = [("John", 25), ("Alice", 30), ("Bob", 28)]
        publish_to_kafka(kafka_broker, kafka_topic, sample_data)
        
        # Assert that the producer function was called with the correct arguments
        mock_producer.assert_called_once_with(kafka_broker, kafka_topic, sample_data)
