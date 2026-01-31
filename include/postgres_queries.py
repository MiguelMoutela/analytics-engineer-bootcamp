import psycopg2 
import os


def execute(query: str):
    conn = psycopg2.connect(
        host=os.environ["PG_HOST"],        # your AWS RDS endpoint
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        port=5432,                         # default
        sslmode="require"                  # AWS RDS usually wants this
    )

    cursor = conn.cursor()
    cursor.execute(query)
    response = cursor.fetchone()
    cursor.close()
    conn.close()
    return response
