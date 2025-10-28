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
    print("Starting periodic Reddit data fetching... (every 10s)")

    while True:
        try:

            data = fetch_reddit_data('astronomy', limit=4)

            # Send one message
            run_producer("reddit_posts", data)

            print(f"[{time.strftime('%H:%M:%S')}] Sent {len(data.get('data', {}).get('children', []))} posts to Kafka.")

            # Keep alive so consumer can print
            time.sleep(5)
        except KeyboardInterrupt:
            print("Stopping data fetch loop...")
            break
        except Exception as e:
            print(f"Error during loop: {e}")
            time.sleep(10)  # small backoff before retrying
"""         finally:
            consumer_thread.join()
 """
if __name__ == "__main__":
    main()
