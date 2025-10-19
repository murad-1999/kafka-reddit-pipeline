import psycopg2
import os 
from dotenv import load_dotenv
from psycopg2 import DatabaseError
import logging

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

def get_connection():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn, conn.cursor()

def setup_database(conn, cursor, table_name):
    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id VARCHAR(10) PRIMARY KEY,     -- Primary Key is essential for ON CONFLICT
        subreddit VARCHAR(50) NOT NULL,
        author VARCHAR(50),
        title TEXT NOT NULL,
        created_utc BIGINT NOT NULL,
        num_comments INTEGER DEFAULT 0
    );
    """
    cursor.execute(CREATE_SQL)
    conn.commit() # Commit the table creation


def insert_post(cursor, post, table_name):
    try:
        query = f"""
            INSERT INTO {table_name} (id, subreddit, author, title, created_utc, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s) 
            ON CONFLICT (id) DO NOTHING;
        """
        values = (
           post["id"],                 # 1. id (for ON CONFLICT)
            post["subreddit"],          # 2. subreddit
            post["author"],             # 3. author
            post["title"],              # 4. title
            post["created_utc"],        # 5. created_utc
            post["num_comments"]        # 6. num_comments
        )

        cursor.execute(query, values)

    except DatabaseError as e:
        logging.error(f"Failed to insert post {post.get('id')}: {e}")
        # Optionally rollback *only this query*:
        cursor.connection.rollback()
        return False
    return True


def commit(conn):
    try:
        conn.commit()
        logging.info("Transaction committed successfully.")
    except DatabaseError as e:
        logging.error(f"commit failed: {e}")
        conn.rollback()
        raise 

def close(conn, cursor):
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Connection closed cleanly.")
    except DatabaseError as e:
        logging.error(f"Error closing connection: {e}")