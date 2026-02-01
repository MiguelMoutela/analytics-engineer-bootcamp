import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from include import postgres_to_snowflake

@dag(
    description="DAG to audit timezone changes from DataExpert.io students",
    default_args={
        "owner": os.environ['DATAEXPERT_DAG_OWNER'],
        "start_date": datetime(2026, 2, 1),
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

    fetch_users = PythonOperator(
        task_id="fetch_users",
        python_callable=postgres_to_snowflake.migrate,
        op_kwargs={
            "source": "STUDENT_API.USERS",
            "target": ".".join([
                os.environ["DATAEXPERT_STUDENT"],
                os.environ["SF_SCHEMA"],
                "USER_TIMEZONE_RAW"
            ])
        }
    )

    fetch_timezone_changes = PythonOperator(
        task_id="fetch_timezone_changes",
        python_callable=postgres_to_snowflake.migrate,
        op_kwargs={
            "source": "STUDENT_API.TIMEZONE_AUDIT_TRACKING",
            "target": ".".join([
                os.environ["DATAEXPERT_STUDENT"],
                os.environ["SF_SCHEMA"],
                "USER_TIMEZONE_AUDIT_RAW"
            ])
        }
    )

    fetch_users >> fetch_timezone_changes

timezone_changes_audit_dag()
