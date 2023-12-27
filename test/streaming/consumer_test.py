from unittest.mock import patch, MagicMock
import pytest
from your_consumer_module import consume_from_kafka  # Import your consumer function

# Mocked Kafka broker details
kafka_broker = "mock_kafka_broker:port"
kafka_topic = "test_topic"

def test_consumer_receives_data():
    # Mocking the Kafka consumer function
    with patch('your_consumer_module.consume_from_kafka') as mock_consumer:
        # Simulating data consumption
        sample_data = [("John", 25), ("Alice", 30), ("Bob", 28)]
        mock_consumer.return_value = sample_data

        # Invoking the consumer function
        received_data = consume_from_kafka(kafka_broker, kafka_topic)

        # Asserting that the consumer received and processed the data
        assert received_data == sample_data
