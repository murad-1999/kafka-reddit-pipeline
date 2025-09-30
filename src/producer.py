from kafka import KafkaProducer

def run_producer(topic: str, message: bytes):
    """Send a message to a Kafka topic."""
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    producer.send(topic, message.encode('utf-8'))
    producer.flush()
    print(f"Sent: {message}")
