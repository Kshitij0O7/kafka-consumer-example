from confluent_kafka import Consumer, KafkaError, KafkaException
import lz4.frame
import uuid
import ssl
from pathlib import Path

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093',
    'group.id': f'trontest1-group-1',  # Generate a unique group ID
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': 'server.cer.pem',
    'ssl.key.location': 'client.key.pem',
    'ssl.certificate.location': 'client.cer.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'usernametest',
    'sasl.password': 'pw',
    'auto.offset.reset': 'latest'
}

# Initialize Kafka consumer
consumer = Consumer(conf)

topic = 'tron.broadcasted.transactions'

# Function to process each message
def process_message(message):
    try:
        buffer = message.value()
        decompressed_value = None

        try:
            # Attempt to decompress LZ4 frame
            decompressed_value = lz4.frame.decompress(buffer).decode('utf-8')
        except Exception as err:
            print(f'LZ4 frame decompression failed: {err}')
            # Fallback to original UTF-8 value if decompression fails
            decompressed_value = buffer.decode('utf-8')

        # Log message data
        log_entry = {
            'partition': message.partition(),
            'offset': message.offset(),
            'value': decompressed_value
        }
        print(log_entry)

    except Exception as err:
        print(f'Error processing message: {err}')

# Subscribe to the topic
consumer.subscribe([topic])

# Poll messages and process them
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer
    consumer.close()
