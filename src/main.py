from producer import run_producer
from consumer import run_consumer
import threading
import time
from fetch_data import fetch_reddit_data
from db_utils import setup_database, get_connection, insert_post, commit, close


def main():
    # Start consumer in background thread
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()

    time.sleep(2)  # give consumer a moment to connect

    data = fetch_reddit_data('astronomy', limit=4)

    # Send one message
    run_producer("test_topic", data)

    conn, cursor = get_connection()
    
    successful_inserts = 0

    # 1. Safely access the list of posts (the 'children' list)
    #    We use .get() here to prevent crashes if the structure is missing
    children_list = data.get('data', {}).get('children', [])

    print(f"Fetched {len(children_list)} posts from Reddit.")
    print(children_list[0].get('data', {})['id'])


    setup_database(conn, cursor, "reddit_posts")
    for post_wrapper in children_list:
        # 2. Extract the actual post data dictionary from the 'data' key of the wrapper
        post_data = post_wrapper.get('data', {})
        
        # 3. Insert the clean dictionary
        if insert_post(cursor, post_data, "reddit_posts"):
            print(post_data['id'])
            successful_inserts += 1

    commit(conn)
    close(conn, cursor)
    print(f"Successfully inserted {successful_inserts} posts!")


    # Keep alive so consumer can print
    time.sleep(5)

if __name__ == "__main__":
    main()
