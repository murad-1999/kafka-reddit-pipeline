from producer import run_producer
from consumer import run_consumer
import threading
import time

def main():
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()

    time.sleep(2)  # give consumer a moment to connect

    # Send one message
    run_producer("test_topic", "Hello")

    # Keep alive so consumer can print
    time.sleep(5)

if __name__ == "__main__":
    main()
