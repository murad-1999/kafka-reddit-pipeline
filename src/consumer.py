from kafka import KafkaConsumer

def run_consumer(topic="test_topic"):
    """Listen to a Kafka topic and print messages."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset="earliest",
        group_id="test_group",
        enable_auto_commit=True,
    )

    print("Consumer listening...")
    for msg in consumer:
        print(f"Received: {msg.value.decode()}")
