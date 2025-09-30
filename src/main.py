from producer import run_producer
from consumer import run_consumer
import threading
import time
from fetch_data import fetch_reddit_data

def main():
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()

    time.sleep(2)  # give consumer a moment to connect

    data = fetch_reddit_data('astronomy', limit=1)

    # Send one message
    run_producer("test_topic", data)

    # Keep alive so consumer can print
    time.sleep(5)

if __name__ == "__main__":
    main()
