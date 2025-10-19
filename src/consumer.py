from kafka import KafkaConsumer
from db_utils import setup_database, get_connection, insert_post, commit, close
import json

def run_consumer(topic="test_topic"):
    """Listen to a Kafka topic and print messages."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset="earliest",
        group_id="test_group",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    print("Consumer listening...")
    conn, cursor = get_connection()
    setup_database(conn, cursor, "reddit_posts")
    successful_inserts = 0
    
    try:
        for msg in consumer:
            print(f"📩 Message received at offset {msg.offset} on topic {msg.topic}")

            data = msg.value  # The JSON object sent by producer

            # Safely extract Reddit posts
            children_list = data.get('data', {}).get('children', [])
            if not children_list:
                print("⚠️ No posts found in message.")
                continue

            for post_wrapper in children_list:
                post_data = post_wrapper.get('data', {})
                try:
                    if insert_post(cursor, post_data, "reddit_posts"):
                        successful_inserts += 1
                        print(f"✅ Inserted post {post_data.get('id')}")
                except Exception as e:
                    conn.rollback()
                    print(f"❌ Failed to insert post: {e}")

            commit(conn)

    except KeyboardInterrupt:
        print("🛑 Consumer stopped manually.")
    finally:
        close(conn, cursor)
        print(f"🔒 Database connection closed. Inserted {successful_inserts} posts total.")