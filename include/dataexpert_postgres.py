from contextlib import contextmanager
import psycopg2
import os

@contextmanager
def connect():
    """Yields connection"""
    
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        port=os.environ["PG_PORT"],
        sslmode="require"
    )
    try:
        yield conn
    finally:
        conn.close()
