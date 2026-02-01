import os
from datetime import datetime, timedelta
import pandas as pd
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
import include.dataexpert_postgres as postgres
import include.dataexpert_snowflake as snowflake


def migrate(source: str, target: str):
    """Postgres to Snowflake migration"""

    with postgres.connect() as pg_cx, snowflake.connect() as sf_cx:
        for chunk in pd.read_sql_table(source, con=pg_cx, chunksize=5000):
            snow_df = sf_cx.create_dataframe(chunk)
            snow_df.write.mode("append").save_as_table(target)
    
    return f"Processed {source} {target}"


@dag(
    description="DAG to audit timezone changes from DataExpert.io students",
    default_args={
        "owner": os.environ['DATAEXPERT_DAG_OWNER'],
        "start_date": datetime(2026, 1, 14),
        "retries": 1,
        "execution_timeout": timedelta(hours=2),
    },
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    tags=["community", "dataexpert.io", "students", "timezone", "snowflake", "postgres"],
)
def timezone_changes_audit_dag():
    """Processes timezone changes from DataExpert.io"""

    def hello():
        print("HELLO AIRFLOW")

    hello_task = PythonOperator(
        task_id="hello",
        python_callable=hello,
    )

    fetch_users_task = PythonOperator(
        task_id="fetch_users",
        python_callable=migrate,
        op_kwargs={
            "source": "STUDENT_API.USERS",
            "target": os.environ["SF_SCHEMA"] + ".USERS_RAW"
        }
    )

    fetch_timezone_changes_task = PythonOperator(
        task_id="fetch_timezone_changes",
        python_callable=migrate,
        op_kwargs={
            "source": "STUDENT_API.TIMEZONE_AUDIT_TRACKING",
            "target": os.environ["SF_SCHEMA"] + ".TIMEZONE_AUDIT_TRACKING_RAW"
        }
    )

    hello_task >> fetch_users_task >> fetch_timezone_changes_task

timezone_changes_audit_dag()
